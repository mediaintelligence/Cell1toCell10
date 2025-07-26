
# Cell 5: Funnel Analysis and Knowledge Graph Creation (Fixed)
# NOTE: Remediation Report suggests performance optimizations (RapidFuzz, async Neo4j, caching)
#       and highlights that this operates on source-like data, missing the
#       Customer Journey Builder step described in the original requirements.

import logging
import os
import json
import time
import random
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, GoogleAPIError
import pandas as pd
import neo4j # Official Neo4j Driver
from neo4j.exceptions import Neo4jError, ServiceUnavailable as Neo4jServiceUnavailable # Import specific Neo4j errors
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm # Progress bar
from typing import List, Dict, Any, Optional, Tuple
import re # For table pattern matching

# Attempt to import fuzzywuzzy, provide fallback
try:
    from fuzzywuzzy import fuzz
    logger.info("Using fuzzywuzzy for table name matching.")
except ImportError:
    logger.warning("fuzzywuzzy library not found. Using simple substring matching for table patterns.")
    # If fuzzywuzzy is not available, create a simple fallback
    class FuzzMock:
        def partial_ratio(self, a: str, b: str) -> int:
            # Simple fallback: check if pattern (a) is a substring of table name (b)
            # This is less flexible than fuzzy matching.
            # Consider installing fuzzywuzzy or python-Levenshtein for better results.
            # Or use RapidFuzz as suggested in remediation report.
            pattern_core = a.replace('\\d{8}', '').strip('.*_') # Basic attempt to get core pattern
            return 100 if pattern_core.lower() in b.lower() else 0
    fuzz = FuzzMock()

# Set up logging (Consider centralizing this as per Remediation Report)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration Class (Consider replacing with Pydantic Settings)
class Config:
    def __init__(self):
        # GCP Config
        self.project_id = os.environ.get('PROJECT_ID', "spry-bus-425315-p6")
        self.dataset_id = os.environ.get('DATASET_ID', "mycocoons")
        self.location = os.environ.get('LOCATION', "US") # BQ Location

        # Neo4j Config (Ensure these are set in your environment)
        self.neo4j_uri = os.environ.get('NEO4J_URI', "neo4j://localhost:7687") # Use neo4j:// or bolt://
        self.neo4j_user = os.environ.get('NEO4J_USER', "neo4j")
        self.neo4j_password = os.environ.get('NEO4J_PASSWORD', "password") # Use strong password!

        # Table Discovery Config (Patterns for relevant tables)
        # Remediation: Use more robust patterns, potentially from config file
        self.table_patterns = {
            # Match tables like 'mycocoons_Shopify_SHP_SALES_20230101_cleaned'
            'shopify_orders': r'mycocoons_Shopify_.*_\d{8}_cleaned$',
            # Match tables like 'mycocoons_GAAnalytics_GAWA_..._20230101_cleaned'
            'google_analytics': r'mycocoons_GAAnalytics_.*_\d{8}_cleaned$',
             # Match tables like 'mycocoons_Facebook_FBADS_..._20230101_cleaned'
            'facebook_ads': r'mycocoons_Facebook_.*_\d{8}_cleaned$',
            # Add patterns for other cleaned source tables if needed
            'google_ads': r'mycocoons_Google_.*_\d{8}_cleaned$',
            'ga4_custom': r'mycocoons_GA4Custom_.*_\d{8}_cleaned$',
        }
        self.fuzzy_threshold = 80 # Threshold for fuzzy matching if regex fails

        # Funnel Config
        self.funnel_stages = self.load_funnel_config()
        # Remediation: Cache this mapping (e.g., Redis)
        self.event_to_stage_map = self._create_event_map()

    def load_funnel_config(self) -> Dict[str, List[str]]:
        """Load funnel configuration from a file or provide defaults."""
        config_file = os.environ.get('FUNNEL_CONFIG', 'funnel_config.json')
        default_config = {
            'awareness': ['ad_view', 'impression', 'page_view', 'site_visit'],
            'interest': ['product_view', 'add_to_cart', 'category_view', 'search'],
            'desire': ['initiate_checkout', 'wishlist_add', 'return_visit', 'form_submit'],
            'action': ['purchase', 'complete_checkout', 'signup', 'order_completed']
        }
        try:
            # Ensure file path is correct if running in different contexts
            base_path = os.path.dirname(os.path.abspath(__file__)) if '__file__' in locals() else '.'
            full_path = os.path.join(base_path, config_file)

            if os.path.exists(full_path):
                with open(full_path, 'r') as file:
                    loaded_config = json.load(file)
                    logger.info(f"Loaded funnel configuration from {full_path}")
                    # Validate structure?
                    if isinstance(loaded_config, dict) and all(isinstance(v, list) for v in loaded_config.values()):
                         return loaded_config
                    else:
                         logger.error(f"Invalid format in funnel configuration file: {full_path}. Using defaults.")
                         return default_config
            else:
                 logger.warning(f"Funnel configuration file '{full_path}' not found. Using default configuration.")
                 # Create a default config file for future runs
                 try:
                     with open(full_path, 'w') as file:
                         json.dump(default_config, file, indent=2)
                         logger.info(f"Created default funnel configuration file: {full_path}")
                 except Exception as e:
                     logger.warning(f"Could not create default funnel configuration file: {e}")
                 return default_config
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing funnel configuration file '{full_path}': {e}. Using defaults.")
            return default_config
        except Exception as e:
            logger.error(f"Unexpected error loading funnel configuration: {e}. Using defaults.")
            return default_config

    def _create_event_map(self) -> Dict[str, str]:
        """Creates a reverse map from event name (lower case) to funnel stage."""
        event_map = {}
        if not self.funnel_stages: return {}
        for stage, events in self.funnel_stages.items():
            for event in events:
                event_lower = event.lower().strip()
                if event_lower in event_map:
                     logger.warning(f"Duplicate event '{event_lower}' found in funnel config. Mapping to stage '{stage}', previously '{event_map[event_lower]}'.")
                event_map[event_lower] = stage
        logger.info(f"Created event-to-stage map with {len(event_map)} event mappings.")
        return event_map

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)

    def set(self, key: str, value: Any) -> None:
        setattr(self, key, value)

config = Config()

# Initialize BQ client (Consider using shared factory)
def get_bigquery_client(max_retries=3):
    """Initialize BigQuery client with retry logic."""
    retry_count = 0
    client = None
    while retry_count < max_retries:
        try:
            client = bigquery.Client(project=config.get('project_id'))
            client.list_datasets(max_results=1) # Test connection
            logger.info("Successfully initialized BigQuery client")
            return client
        except Exception as e:
            retry_count += 1
            wait_time = 2 ** retry_count
            logger.warning(f"Error initializing BigQuery client: {e}. Retrying in {wait_time} seconds. Attempt {retry_count}/{max_retries}")
            time.sleep(wait_time)
    logger.error("Failed to initialize BigQuery client after multiple attempts.")
    raise RuntimeError("Failed to initialize BigQuery client after multiple attempts")

# Custom Exceptions
class TableNotFoundException(Exception):
    """Exception raised when a table is not found."""
    pass

class DataValidationError(Exception):
    """Exception raised when data validation fails."""
    pass

# Retry Decorator (Consider extracting to shared utils)
# Include Neo4j specific retryable errors
NEO4J_RETRYABLE_EXCEPTIONS = (
    Neo4jServiceUnavailable, # e.g., Neo.TransientError.General.ServiceUnavailable
    Neo4jError, # Catch broader Neo4j errors, check codes if needed
    ConnectionRefusedError,
    TimeoutError,
)
ALL_RETRYABLE_EXCEPTIONS = (GoogleAPIError, ConnectionError, TimeoutError) + NEO4J_RETRYABLE_EXCEPTIONS

def retry_with_backoff(retries=3, backoff_in_seconds=1):
    """Decorator for retrying functions with exponential backoff."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            x = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except ALL_RETRYABLE_EXCEPTIONS as e:
                    # Check Neo4jError codes specifically if needed
                    is_retryable = True
                    if isinstance(e, Neo4jError):
                         # Example: Only retry specific transient errors
                         # if not e.code.startswith("Neo.TransientError"): is_retryable = False
                         pass # For now, retry most Neo4jErrors

                    if is_retryable and x < retries:
                        sleep = (backoff_in_seconds * 2 ** x + random.uniform(0, 1))
                        logger.warning(f"Retrying {func.__name__} in {sleep:.2f} seconds due to error: {e}")
                        time.sleep(sleep)
                        x += 1
                    else:
                        logger.error(f"Function {func.__name__} failed after {x} retries: {e}", exc_info=True)
                        raise
                except Exception as e:
                     logger.error(f"Function {func.__name__} encountered non-retryable error: {e}", exc_info=True)
                     raise
        return wrapper
    return decorator

# --- BigQuery Table Discovery and Processing ---

@retry_with_backoff()
def discover_tables(client: bigquery.Client, dataset_id: str, table_patterns: Dict[str, str], fuzzy_threshold: int) -> Dict[str, List[str]]:
    """Discovers relevant *_cleaned tables in the dataset based on regex and fuzzy matching."""
    discovered_tables = {table_type: [] for table_type in table_patterns}
    logger.info(f"Discovering tables in dataset {dataset_id} matching patterns...")

    try:
        dataset_ref = client.dataset(dataset_id)
        tables_iterator = client.list_tables(dataset_ref)
        # Filter for actual tables first
        all_tables = [table for table in tables_iterator if table.table_type == 'TABLE']

        if not all_tables:
            logger.warning(f"No tables found in dataset {dataset_id}")
            return discovered_tables

        logger.info(f"Found {len(all_tables)} tables in dataset {dataset_id}. Applying patterns...")

        # Compile regex patterns for efficiency
        compiled_patterns = {
            table_type: re.compile(pattern, re.IGNORECASE)
            for table_type, pattern in table_patterns.items()
        }

        matched_tables = set() # Keep track of tables already matched by regex
        # First pass: Regex matching
        for table in all_tables:
            for table_type, compiled_regex in compiled_patterns.items():
                if compiled_regex.match(table.table_id):
                    discovered_tables[table_type].append(table.table_id)
                    matched_tables.add(table.table_id)
                    logger.debug(f"Regex matched table '{table.table_id}' to type '{table_type}'")
                    break # Move to next table once matched

        # Second pass: Fuzzy matching for unmatched tables (if fuzzywuzzy is available)
        if fuzz.__class__.__name__ != 'FuzzMock':
             unmatched_tables = [table for table in all_tables if table.table_id not in matched_tables and table.table_id.endswith('_cleaned')]
             if unmatched_tables:
                  logger.info(f"Attempting fuzzy matching for {len(unmatched_tables)} unmatched *_cleaned tables...")
                  for table in unmatched_tables:
                       best_match_score = 0
                       best_match_type = None
                       for table_type, pattern_str in table_patterns.items():
                            # Use partial_ratio for flexibility
                            score = fuzz.partial_ratio(pattern_str, table.table_id)
                            if score > best_match_score:
                                 best_match_score = score
                                 best_match_type = table_type

                       if best_match_type and best_match_score >= fuzzy_threshold:
                            logger.info(f"Fuzzy matched table '{table.table_id}' to type '{best_match_type}' with score {best_match_score}")
                            discovered_tables[best_match_type].append(table.table_id)
                       # else: logger.debug(f"No fuzzy match found for '{table.table_id}' above threshold {fuzzy_threshold}")

        # Log discovery results
        total_discovered = 0
        for table_type, tables in discovered_tables.items():
            count = len(tables)
            total_discovered += count
            if count > 0:
                logger.info(f"Discovered {count} tables for type '{table_type}'.")
            else:
                logger.warning(f"No tables discovered for type '{table_type}' matching pattern: {table_patterns[table_type]}")

        logger.info(f"Total relevant tables discovered: {total_discovered}")
        return discovered_tables
    except NotFound:
        logger.error(f"Dataset {dataset_id} not found in project {config.project_id}")
        raise TableNotFoundException(f"Dataset {dataset_id} not found")
    except Exception as e:
        logger.error(f"Error discovering tables in dataset {dataset_id}: {e}", exc_info=True)
        raise

@retry_with_backoff()
def get_table_schema(client: bigquery.Client, full_table_id: str) -> List[bigquery.SchemaField]:
    """Retrieves the schema of a BigQuery table."""
    try:
        table = client.get_table(full_table_id)
        return table.schema
    except NotFound:
        raise TableNotFoundException(f"Table {full_table_id} not found")
    except Exception as e:
        logger.error(f"Error retrieving schema for table {full_table_id}: {e}", exc_info=True)
        raise

def generate_query_for_kg(table_id: str, schema: List[bigquery.SchemaField]) -> Optional[str]:
    """Generates a BigQuery SQL query selecting columns relevant for KG/Funnel."""
    # Remediation: Use BQ Scripting UDF for more complex logic?
    if not schema:
        logger.warning(f"No schema found for table {table_id}. Cannot generate query.")
        return None

    # Identify essential columns (case-insensitive check)
    schema_cols_lower = {field.name.lower(): field.name for field in schema}

    # Core columns needed for basic KG/Funnel
    customer_id_col = schema_cols_lower.get('customer_id', schema_cols_lower.get('unified_id')) # Use unified_id as fallback
    event_col = schema_cols_lower.get('event', schema_cols_lower.get('event_name')) # Use event_name as fallback
    timestamp_col = schema_cols_lower.get('event_timestamp', schema_cols_lower.get('event_datetime', schema_cols_lower.get('timestamp', schema_cols_lower.get('created_at', schema_cols_lower.get('date')))))

    if not customer_id_col:
        logger.warning(f"Required 'customer_id' or 'unified_id' column not found in table {table_id}. KG export might be limited.")
        # Decide: Skip table or proceed with limited data? Proceeding for now.
        # We might generate a placeholder ID later if needed.
    if not event_col:
         logger.warning(f"Required 'event' or 'event_name' column not found in table {table_id}. Funnel mapping might fail.")
         # Proceeding, will try to infer event later if possible.
    if not timestamp_col:
         logger.warning(f"Timestamp column ('event_timestamp', 'created_at', etc.) not found in table {table_id}. Temporal analysis limited.")
         # Proceeding without timestamp.

    # Select essential columns plus potentially others useful for KG properties
    select_cols = []
    if customer_id_col: select_cols.append(f"`{customer_id_col}` AS customer_id")
    if event_col: select_cols.append(f"`{event_col}` AS event_name")
    if timestamp_col: select_cols.append(f"`{timestamp_col}` AS event_timestamp")

    # Add other potentially useful columns (adjust as needed)
    other_cols = ['source', 'medium', 'campaign', 'ad_id', 'product_id', 'order_id', 'total_price', 'value']
    for col in other_cols:
        bq_col = schema_cols_lower.get(col)
        if bq_col and bq_col not in [customer_id_col, event_col, timestamp_col]:
            select_cols.append(f"`{bq_col}` AS {col}") # Alias to standard name

    if not select_cols:
         logger.error(f"Could not identify any relevant columns (customer_id, event, timestamp) in table {table_id}. Skipping query generation.")
         return None

    # Generate the query
    query = f"SELECT {', '.join(select_cols)} FROM `{config.project_id}.{config.dataset_id}.{table_id}`"

    # Add filtering? e.g., WHERE event_timestamp > '...'
    # query += " WHERE event_timestamp IS NOT NULL" # Example filter

    # Add limit for testing?
    # query += " LIMIT 10000"

    logger.debug(f"Generated query for {table_id}: {query}")
    return query

@retry_with_backoff()
def process_table_for_kg(client: bigquery.Client, table_id: str) -> Optional[pd.DataFrame]:
    """Reads and prepares data from a single table for KG/Funnel processing."""
    full_table_id = f"{config.project_id}.{config.dataset_id}.{table_id}"
    logger.debug(f"Processing table {table_id} for KG/Funnel...")
    try:
        # Get schema
        schema = get_table_schema(client, full_table_id)

        # Generate query based on schema
        query = generate_query_for_kg(table_id, schema)
        if query is None:
            logger.warning(f"Skipping table {table_id} due to missing relevant columns.")
            return None

        # Execute query
        logger.debug(f"Executing KG query for table {table_id}")
        query_job = client.query(query)
        df = query_job.to_dataframe(timeout=600) # 10 min timeout

        if df.empty:
            logger.info(f"Table {table_id} returned no data for KG query.")
            return None

        # --- Post-Query Data Preparation ---
        # Ensure essential columns exist, even if NULL (makes downstream processing easier)
        if 'customer_id' not in df.columns: df['customer_id'] = pd.NA
        if 'event_name' not in df.columns: df['event_name'] = pd.NA
        if 'event_timestamp' not in df.columns: df['event_timestamp'] = pd.NaT

        # Fill missing customer_id with a placeholder (if needed, depends on KG strategy)
        # Example: Use hash of other columns if customer_id is missing
        missing_cust_id = df['customer_id'].isna()
        if missing_cust_id.any():
             logger.warning(f"{missing_cust_id.sum()} rows in {table_id} missing customer_id. Generating placeholder.")
             # Simple placeholder - consider a more robust hashing strategy if needed
             df.loc[missing_cust_id, 'customer_id'] = 'placeholder_' + df[missing_cust_id].index.astype(str)

        # Convert timestamp to consistent type (e.g., UTC datetime)
        if 'event_timestamp' in df.columns:
             df['event_timestamp'] = pd.to_datetime(df['event_timestamp'], errors='coerce', utc=True)

        # Add source table info
        df['source_table'] = table_id

        logger.info(f"Successfully processed table {table_id} for KG. Shape: {df.shape}")
        return df

    except TableNotFoundException:
        logger.warning(f"Skipping table {table_id} as it was not found during processing.")
        return None # Return None if table not found
    except Exception as e:
        logger.error(f"Error processing table {table_id} for KG: {e}", exc_info=True)
        raise # Reraise error to be caught by parallel processor

# --- Neo4j Export ---
# Remediation: Use neo4j.AsyncGraphDatabase and run in asyncio event loop

class Neo4jExporter:
    def __init__(self, uri: str, user: str, password: str, max_retries: int = 3):
        """Initialize connection to Neo4j database with retry logic."""
        self.uri = uri
        self.user = user
        self._password = password # Store password privately
        self.driver = None
        self._connect(max_retries)

    @retry_with_backoff(retries=5, backoff_in_seconds=2) # More aggressive retry for connection
    def _connect(self, max_retries: int):
        """Connect to Neo4j with retry logic."""
        # Close existing driver if reconnecting
        if self.driver:
            try:
                self.driver.close()
            except Exception as e:
                logger.warning(f"Error closing existing Neo4j driver: {e}")
            self.driver = None

        logger.info(f"Attempting to connect to Neo4j at {self.uri}...")
        # Use try-except to catch connection errors during initialization
        try:
            # Adjust config: disable encryption if using bolt://localhost without certs
            encrypted = self.uri.startswith("neo4j://")
            self.driver = neo4j.GraphDatabase.driver(
                self.uri,
                auth=(self.user, self._password),
                encrypted=encrypted, # Auto-detect based on scheme often works
                max_connection_lifetime=3600 # Optional: Refresh connections periodically
            )
            # Verify connection is alive
            self.driver.verify_connectivity()
            logger.info(f"Successfully connected to Neo4j at {self.uri}")
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {e}", exc_info=True)
            self.driver = None
            raise # Reraise to trigger retry decorator

    def close(self):
        """Close the Neo4j connection."""
        if self.driver:
            try:
                self.driver.close()
                logger.info("Neo4j connection closed.")
                self.driver = None
            except Exception as e:
                logger.error(f"Error closing Neo4j driver: {e}")

    def _get_session(self, database: Optional[str] = None) -> neo4j.Session:
        """Gets a Neo4j session, ensuring driver is connected."""
        if not self.driver:
            logger.warning("Neo4j driver not connected. Attempting to reconnect...")
            self._connect(max_retries=3) # Try to reconnect
            if not self.driver:
                 raise ConnectionError("Neo4j driver is not connected.")
        # Specify database if needed (e.g., for AuraDB)
        return self.driver.session(database=database or neo4j.DEFAULT_DATABASE)

    @retry_with_backoff()
    def run_cypher(self, query: str, parameters: Optional[Dict] = None) -> List[neo4j.Record]:
        """Runs a Cypher query within a managed transaction with retries."""
        logger.debug(f"Running Cypher: {query[:100]}... Params: {parameters is not None}")
        try:
            with self._get_session() as session:
                # Use execute_read or execute_write for automatic transaction management and retries
                # Decide based on query type (heuristic: CREATE/MERGE/SET/DELETE -> write)
                is_write_query = any(kw in query.upper() for kw in ["CREATE", "MERGE", "SET", "DELETE", "REMOVE"])
                if is_write_query:
                    result = session.execute_write(lambda tx: tx.run(query, parameters or {}).data())
                else:
                    result = session.execute_read(lambda tx: tx.run(query, parameters or {}).data())
                return result
        except Exception as e:
            logger.error(f"Error executing Cypher query: {e}", exc_info=True)
            raise # Reraise after logging

    # Remediation: Batch size 500 suggested
    def batch_write_nodes(self, nodes: List[Dict[str, Any]], label: str, batch_size: int = 500, merge_key: str = 'id') -> int:
        """Writes nodes in batches using MERGE."""
        if not nodes: return 0
        if not self.driver: raise ConnectionError("Neo4j driver not connected.")

        total_created_or_merged = 0
        logger.info(f"Writing {len(nodes)} nodes with label '{label}' in batches of {batch_size}...")

        # Ensure merge_key exists in all nodes
        for node in nodes:
             if merge_key not in node or pd.isna(node[merge_key]):
                  # Generate a fallback ID if merge key is missing/NA
                  fallback_id = f"{label}_{hashlib.md5(json.dumps(node, sort_keys=True).encode()).hexdigest()}"
                  node[merge_key] = fallback_id
                  logger.debug(f"Generated fallback ID '{fallback_id}' for node in label '{label}'")

        query = f"""
        UNWIND $batch AS node_props
        MERGE (n:{label} {{{merge_key}: node_props.{merge_key}}})
        ON CREATE SET n = node_props
        ON MATCH SET n += node_props // Update properties on match
        // Remove the merge key from properties being set if it's part of node_props but shouldn't be overwritten
        // SET n += apoc.map.removeKey(node_props, $merge_key_param)
        RETURN count(n) AS count
        """
        # parameters = {"merge_key_param": merge_key} # If using removeKey

        for i in tqdm(range(0, len(nodes), batch_size), desc=f"Writing {label} nodes"):
            batch = nodes[i:i + batch_size]
            try:
                # Pass merge_key_param if using removeKey in query
                # result = self.run_cypher(query, parameters={"batch": batch, **parameters})
                result = self.run_cypher(query, parameters={"batch": batch})
                count = result[0]['count'] if result else 0
                total_created_or_merged += count
            except Exception as e:
                 logger.error(f"Error writing node batch {i//batch_size + 1} for label {label}: {e}", exc_info=True)
                 # Decide: Stop or continue? Continuing for now.
                 # Consider adding failed batches to a retry queue.

        logger.info(f"Finished writing nodes for label '{label}'. Total created/merged: {total_created_or_merged}")
        return total_created_or_merged

    def batch_write_relationships(self, relationships: List[Dict[str, Any]], batch_size: int = 500) -> int:
        """Writes relationships in batches using MERGE."""
        if not relationships: return 0
        if not self.driver: raise ConnectionError("Neo4j driver not connected.")

        total_created_or_merged = 0
        logger.info(f"Writing {len(relationships)} relationships in batches of {batch_size}...")

        # Query using MERGE for relationships (requires unique properties or relies on node uniqueness)
        # Assumes nodes already exist. Define start/end node labels and keys.
        # This query assumes start/end nodes have a common 'id' property and potentially different labels.
        # Modify MATCH clauses if nodes have different primary keys or labels.
        query = """
        UNWIND $batch AS rel_data
        MATCH (start_node {id: rel_data.start_node_id})
        MATCH (end_node {id: rel_data.end_node_id})
        MERGE (start_node)-[r:RELATIONSHIP {type: rel_data.type}]->(end_node) // Example: Merge based on type property
        // Or MERGE (start_node)-[r:REL_TYPE {unique_prop: rel_data.unique_prop}]->(end_node) if using specific type and property
        ON CREATE SET r = rel_data.properties
        ON MATCH SET r += rel_data.properties // Update properties on match
        RETURN count(r) AS count
        """
        # Alternative using dynamic type with APOC (if available)
        apoc_query = """
        UNWIND $batch AS rel_data
        MATCH (start_node {id: rel_data.start_node_id})
        MATCH (end_node {id: rel_data.end_node_id})
        CALL apoc.merge.relationship(
            start_node,
            rel_data.type, // Relationship type string
            {id: rel_data.rel_id}, // Properties to MERGE on (e.g., unique rel ID)
            rel_data.properties, // Properties to SET on CREATE/MATCH
            end_node
        ) YIELD relationship
        RETURN count(relationship) AS count
        """
        # Choose query based on APOC availability or preference
        chosen_query = apoc_query # Assuming APOC is available

        for i in tqdm(range(0, len(relationships), batch_size), desc="Writing relationships"):
            batch = relationships[i:i + batch_size]
            # Prepare batch data structure for the chosen query
            prepared_batch = []
            for rel in batch:
                 # Ensure required fields exist
                 if not all(k in rel for k in ['start_node_id', 'end_node_id', 'type']):
                      logger.warning(f"Skipping invalid relationship data: {rel}")
                      continue
                 # Structure for APOC query
                 prepared_batch.append({
                      "start_node_id": rel['start_node_id'],
                      "end_node_id": rel['end_node_id'],
                      "type": rel['type'].upper().replace(' ','_'), # Sanitize type
                      "rel_id": rel.get('rel_id', f"{rel['start_node_id']}_{rel['type']}_{rel['end_node_id']}"), # Example unique ID for merge
                      "properties": rel.get('properties', {})
                 })

            if not prepared_batch: continue

            try:
                result = self.run_cypher(chosen_query, parameters={"batch": prepared_batch})
                count = result[0]['count'] if result else 0
                total_created_or_merged += count
            except Exception as e:
                 # Check if error is due to APOC not found, potentially fallback
                 if "Unknown function 'apoc.merge.relationship'" in str(e):
                      logger.warning("APOC not found. Consider installing APOC plugin or using standard MERGE query.")
                      # Optionally: Fallback to standard MERGE here (would require different query/batch structure)
                 logger.error(f"Error writing relationship batch {i//batch_size + 1}: {e}", exc_info=True)
                 # Decide: Stop or continue? Continuing for now.

        logger.info(f"Finished writing relationships. Total created/merged: {total_created_or_merged}")
        return total_created_or_merged

# --- Funnel Logic and KG Construction ---

def map_event_to_funnel_stage(event_name: Optional[str]) -> Optional[str]:
    """Maps an event name to a funnel stage using the precomputed map."""
    if not event_name or pd.isna(event_name):
        return None
    # Use the map created in Config
    return config.event_to_stage_map.get(str(event_name).lower().strip())

def create_knowledge_graph_nodes_rels(all_data: Dict[str, pd.DataFrame]) -> Tuple[Dict[str, List[Dict]], List[Dict]]:
    """Transforms DataFrames into lists of nodes and relationships for Neo4j."""
    all_nodes = defaultdict(list)
    all_relationships = []

    logger.info("Transforming DataFrame data into Neo4j nodes and relationships...")

    # Define node types and primary keys (example)
    # This structure should align with the desired KG model
    node_definitions = {
        'Customer': {'key': 'customer_id'},
        'Event': {'key': 'event_id'}, # Need to generate unique event IDs
        'Session': {'key': 'session_id'}, # Need session info
        'Product': {'key': 'product_id'},
        'Order': {'key': 'order_id'},
        'Campaign': {'key': 'campaign_id'},
        'Ad': {'key': 'ad_id'},
        'FunnelStage': {'key': 'stage_name'},
    }

    processed_entities = {label: set() for label in node_definitions} # Track created nodes by primary key

    # Process each source DataFrame
    for source_type, df in all_data.items():
        if df is None or df.empty:
            continue
        logger.debug(f"Processing {len(df)} rows from source '{source_type}' for KG transformation...")

        df = df.where(pd.notna(df), None) # Replace NaT/NA with None for JSON compatibility
        records = df.to_dict('records')

        for record in records:
            # --- Create/Identify Nodes ---
            customer_node = None
            if record.get('customer_id'):
                cust_id = str(record['customer_id'])
                if cust_id not in processed_entities['Customer']:
                     customer_node = {'id': cust_id, 'customer_id': cust_id}
                     # Add other customer properties if available
                     all_nodes['Customer'].append(customer_node)
                     processed_entities['Customer'].add(cust_id)
                else:
                     customer_node = {'id': cust_id} # Reference existing node

            # Generate a unique ID for each event instance
            # Example: hash of record content + timestamp
            event_hash = hashlib.md5(json.dumps(record, sort_keys=True, default=str).encode()).hexdigest()
            event_id = f"evt_{record.get('source_table', 'unknown')}_{event_hash}"
            event_node = {'id': event_id, 'event_id': event_id}
            event_node.update({k: v for k, v in record.items() if v is not None}) # Add all record props
            all_nodes['Event'].append(event_node)
            # No need to track processed_entities for Event if always creating new

            # Identify other nodes (Product, Order, Campaign, Ad) based on available IDs
            other_nodes = {}
            for label, definition in node_definitions.items():
                 if label in ['Customer', 'Event', 'FunnelStage', 'Session']: continue # Handled separately
                 key_col = definition['key']
                 entity_id = record.get(key_col)
                 if entity_id:
                      entity_id_str = str(entity_id)
                      if entity_id_str not in processed_entities[label]:
                           node = {'id': entity_id_str, key_col: entity_id_str}
                           # Add other relevant properties from record?
                           all_nodes[label].append(node)
                           processed_entities[label].add(entity_id_str)
                      other_nodes[label] = {'id': entity_id_str} # Reference node

            # --- Create Relationships ---
            # 1. Customer -> PERFORMED -> Event
            if customer_node:
                 all_relationships.append({
                      'start_node_id': customer_node['id'],
                      'end_node_id': event_node['id'],
                      'type': 'PERFORMED',
                      'properties': {'timestamp': record.get('event_timestamp')}
                 })

            # 2. Event -> PART_OF -> Session (Requires session_id)
            # if record.get('session_id'): ... create Session node and relationship ...

            # 3. Event -> RELATED_TO -> Product/Order/Campaign/Ad
            for label, node_ref in other_nodes.items():
                 all_relationships.append({
                      'start_node_id': event_node['id'],
                      'end_node_id': node_ref['id'],
                      'type': f'RELATED_TO_{label.upper()}', # Or more specific type
                      'properties': {'timestamp': record.get('event_timestamp')}
                 })

            # 4. Event -> HAS_STAGE -> FunnelStage
            event_name = record.get('event_name')
            stage_name = map_event_to_funnel_stage(event_name)
            if stage_name:
                 stage_id = stage_name.lower().replace(' ', '_')
                 if stage_id not in processed_entities['FunnelStage']:
                      stage_node = {'id': stage_id, 'stage_name': stage_name}
                      all_nodes['FunnelStage'].append(stage_node)
                      processed_entities['FunnelStage'].add(stage_id)

                 all_relationships.append({
                      'start_node_id': event_node['id'],
                      'end_node_id': stage_id,
                      'type': 'HAS_STAGE',
                      'properties': {'timestamp': record.get('event_timestamp')}
                 })

    # Deduplicate nodes before returning (important if nodes are added multiple times)
    final_nodes = {}
    for label, nodes in all_nodes.items():
         unique_nodes = list({node['id']: node for node in nodes}.values())
         final_nodes[label] = unique_nodes
         logger.info(f"Prepared {len(unique_nodes)} unique nodes for label '{label}'.")

    logger.info(f"Prepared {len(all_relationships)} relationships.")
    return final_nodes, all_relationships


def calculate_funnel_metrics(all_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """Calculates funnel metrics based on the processed data."""
    if not all_data or not config.funnel_stages:
        logger.warning("No data or funnel stages provided for metric calculation.")
        return {}

    # Use precomputed event map
    event_map = config.event_to_stage_map
    stage_order = list(config.funnel_stages.keys())

    # Track unique users per stage {stage_name: set(customer_id)}
    users_per_stage = {stage: set() for stage in stage_order}
    stage_event_counts = {stage: 0 for stage in stage_order}

    logger.info("Calculating funnel metrics...")
    total_events_processed = 0
    events_mapped_to_stage = 0

    for source_type, df in all_data.items():
        if df is None or df.empty: continue
        if 'customer_id' not in df.columns or 'event_name' not in df.columns:
             logger.warning(f"Skipping metric calculation for source '{source_type}': Missing customer_id or event_name.")
             continue

        # Iterate over rows efficiently
        for _, row in df.iterrows():
            total_events_processed += 1
            customer_id = row['customer_id']
            event_name = row['event_name']

            if pd.isna(customer_id) or pd.isna(event_name): continue

            stage = event_map.get(str(event_name).lower().strip())
            if stage:
                events_mapped_to_stage += 1
                users_per_stage[stage].add(customer_id)
                stage_event_counts[stage] += 1

    logger.info(f"Processed {total_events_processed} events for metrics. {events_mapped_to_stage} events mapped to a funnel stage.")

    # Calculate metrics
    metrics = {}
    previous_stage_users = 0
    for i, stage in enumerate(stage_order):
        current_users_set = users_per_stage[stage]
        current_users_count = len(current_users_set)

        # Users entering this stage (unique users in this stage not in previous stages)
        # This definition might be too strict, often funnel looks at total unique users *up to* this stage.
        # Let's calculate total unique users *at* this stage first.
        metrics[stage] = {
            'stage_index': i,
            'unique_users': current_users_count,
            'total_events': stage_event_counts[stage],
            'conversion_from_previous': 0.0,
            'drop_off_from_previous': 1.0,
        }

        if i == 0:
            previous_stage_users = current_users_count
        elif previous_stage_users > 0:
            # Conversion rate: unique users in current stage / unique users in previous stage
            # This measures stage-to-stage conversion based on unique users reaching each stage.
            conversion = current_users_count / previous_stage_users
            metrics[stage]['conversion_from_previous'] = conversion
            metrics[stage]['drop_off_from_previous'] = 1.0 - conversion
            # Update previous_stage_users for the next iteration *only if* current stage has users
            # This prevents division by zero if a stage has 0 users.
            if current_users_count > 0:
                 previous_stage_users = current_users_count
            # If current stage has 0 users, the conversion to the *next* stage from the *last non-zero stage* should be 0.
            # The current logic handles this by keeping previous_stage_users non-zero until a stage with users is hit.

        # Ensure drop-off isn't negative due to floating point issues
        metrics[stage]['drop_off_from_previous'] = max(0.0, metrics[stage]['drop_off_from_previous'])


    # Calculate overall conversion (first stage to last stage)
    first_stage = stage_order[0]
    last_stage = stage_order[-1]
    first_stage_users = metrics[first_stage]['unique_users']
    last_stage_users = metrics[last_stage]['unique_users']

    overall_conversion = (last_stage_users / first_stage_users) if first_stage_users > 0 else 0.0
    metrics['overall'] = {
        'start_stage': first_stage,
        'end_stage': last_stage,
        'start_stage_users': first_stage_users,
        'end_stage_users': last_stage_users,
        'overall_conversion_rate': overall_conversion,
    }

    logger.info("Funnel metrics calculated.")
    return metrics

# --- Main Orchestration ---

def main():
    """Main function for Cell 5."""
    logger.info("Starting Funnel Analysis and Knowledge Graph Creation (Cell 5)...")
    start_time = time.time()
    bq_client = None
    neo4j_exporter = None

    try:
        # Initialize BigQuery client
        bq_client = get_bigquery_client()

        # 1. Discover relevant tables
        discovered_tables_map = discover_tables(
            client=bq_client,
            dataset_id=config.get('dataset_id'),
            table_patterns=config.get('table_patterns'),
            fuzzy_threshold=config.get('fuzzy_threshold')
        )

        all_discovered_table_ids = [tbl for tables in discovered_tables_map.values() for tbl in tables]
        if not all_discovered_table_ids:
            logger.warning("No relevant tables discovered based on patterns. Cannot proceed.")
            return

        # 2. Process tables in parallel to get DataFrames
        all_data: Dict[str, Optional[pd.DataFrame]] = {}
        logger.info(f"Processing {len(all_discovered_table_ids)} discovered tables for KG/Funnel data...")
        with ThreadPoolExecutor(max_workers=5) as executor: # Adjust workers
            futures = {
                executor.submit(process_table_for_kg, bq_client, table_id): table_id
                for table_id in all_discovered_table_ids
            }
            for future in tqdm(as_completed(futures), total=len(futures), desc="Reading BQ Tables"):
                table_id = futures[future]
                try:
                    df = future.result()
                    # Store DataFrame, find original source type based on table_id
                    source_type = "unknown"
                    for type_key, id_list in discovered_tables_map.items():
                         if table_id in id_list:
                              source_type = type_key
                              break
                    # Combine dataframes of the same source type if needed, or store individually
                    # Storing individually for now, keyed by table_id
                    all_data[table_id] = df # Store df or None
                except Exception as e:
                    logger.error(f"Failed to process table {table_id}: {e}", exc_info=True)
                    all_data[table_id] = None # Mark as failed

        # Combine dataframes by source type for analysis (optional, depends on analysis needs)
        combined_data_by_source: Dict[str, pd.DataFrame] = defaultdict(list)
        valid_data_found = False
        for table_id, df in all_data.items():
             if df is not None and not df.empty:
                  valid_data_found = True
                  # Find source type again
                  source_type = "unknown"
                  for type_key, id_list in discovered_tables_map.items():
                       if table_id in id_list:
                            source_type = type_key
                            break
                  combined_data_by_source[source_type].append(df)

        if not valid_data_found:
             logger.error("No valid data could be processed from discovered tables. Exiting.")
             return

        # Concatenate lists into single DataFrames per source type
        final_data_for_analysis: Dict[str, pd.DataFrame] = {}
        for source_type, df_list in combined_data_by_source.items():
             if df_list:
                  try:
                       final_data_for_analysis[source_type] = pd.concat(df_list, ignore_index=True)
                       logger.info(f"Combined {len(df_list)} dataframes for source '{source_type}'. Total rows: {len(final_data_for_analysis[source_type])}")
                  except Exception as concat_err:
                       logger.error(f"Error concatenating dataframes for source '{source_type}': {concat_err}")


        # 3. Calculate Funnel Metrics
        funnel_metrics = calculate_funnel_metrics(final_data_for_analysis)
        if funnel_metrics:
            logger.info("--- Funnel Metrics ---")
            if 'overall' in funnel_metrics:
                 overall = funnel_metrics['overall']
                 logger.info(f"Overall Conversion ({overall['start_stage']} -> {overall['end_stage']}): "
                             f"{overall['overall_conversion_rate']:.2%} "
                             f"({overall['end_stage_users']}/{overall['start_stage_users']} users)")
            for stage, metrics_data in funnel_metrics.items():
                 if stage != 'overall':
                      logger.info(f"Stage '{stage}' (Index {metrics_data['stage_index']}): "
                                  f"Unique Users: {metrics_data['unique_users']}, "
                                  f"Total Events: {metrics_data['total_events']}, "
                                  f"Conv. from Prev: {metrics_data['conversion_from_previous']:.2%}, "
                                  f"Dropoff from Prev: {metrics_data['drop_off_from_previous']:.2%}")
            logger.info("----------------------")
        else:
            logger.warning("Funnel metrics calculation did not produce results.")


        # 4. Prepare data for Neo4j
        kg_nodes, kg_relationships = create_knowledge_graph_nodes_rels(final_data_for_analysis)

        # 5. Export to Neo4j
        if not kg_nodes and not kg_relationships:
             logger.warning("No nodes or relationships generated for Neo4j export.")
        else:
             logger.info("Initializing Neo4j Exporter...")
             try:
                  neo4j_exporter = Neo4jExporter(
                       config.get('neo4j_uri'),
                       config.get('neo4j_user'),
                       config.get('neo4j_password')
                  )

                  # Export nodes by label
                  for label, nodes in kg_nodes.items():
                       if nodes:
                            neo4j_exporter.batch_write_nodes(nodes, label, batch_size=500) # Use batch size 500

                  # Export relationships
                  if kg_relationships:
                       neo4j_exporter.batch_write_relationships(kg_relationships, batch_size=500) # Use batch size 500

                  logger.info("Neo4j export process completed.")

             except ConnectionError as conn_err:
                  logger.error(f"Could not connect to Neo4j. Skipping export. Error: {conn_err}")
             except Exception as neo_err:
                  logger.error(f"An error occurred during Neo4j export: {neo_err}", exc_info=True)
             finally:
                  if neo4j_exporter:
                       neo4j_exporter.close()

    except Exception as e:
        logger.critical(f"A critical error occurred in the main Cell 5 process: {e}", exc_info=True)
    finally:
        # Close Neo4j driver if initialized and not closed
        if neo4j_exporter and neo4j_exporter.driver:
             neo4j_exporter.close()
        end_time = time.time()
        logger.info(f"Funnel Analysis and Knowledge Graph Creation (Cell 5) finished in {end_time - start_time:.2f} seconds.")


if __name__ == "__main__":
    main()
