# Cell 3: Cleaning Service (DEPLOYMENT READY v4 - Syntax Fixes & Shared Funcs Included)
# NOTE: Deploy as Cloud Function/Run service triggered by 'etl-cleaning-requests' topic.
#       Ensure 'etl-catalog-requests' topic exists for triggering Cell 4.
#       Includes shared naming/retry/client functions FOR NOW (move to utils for deployment).
#       REMOVED if __name__ == "__main__" block for deployment readiness.

import pandas as pd
from google.cloud import bigquery, pubsub_v1
import logging
# Use specific exceptions for clarity
from google.api_core.exceptions import NotFound, GoogleAPIError, ServiceUnavailable, BadRequest
from google.cloud import exceptions as google_exceptions
import time
import os
import random
import json
import base64
import re
import inspect
from typing import Dict, Any, Optional, Callable, List, Tuple
from tenacity import retry as tenacity_retry, stop_after_attempt, wait_exponential, retry_if_exception_type as tenacity_if_exception_type

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s')
logger = logging.getLogger(__name__)
# (Reduce verbosity logs)
logging.getLogger('google.api_core').setLevel(logging.WARNING); logging.getLogger('google.auth').setLevel(logging.WARNING)
logging.getLogger('google.cloud').setLevel(logging.WARNING); logging.getLogger('urllib3').setLevel(logging.WARNING)

# --- Configuration ---
PROJECT_ID = os.environ.get('PROJECT_ID', "spry-bus-425315-p6")
DATASET_ID = os.environ.get('DATASET_ID', "mycocoons")
LOCATION = os.environ.get('LOCATION', "US") # BQ Location
CATALOG_PUBSUB_TOPIC_ID = os.environ.get('PUBSUB_CATALOG_TOPIC', 'etl-catalog-requests')

# --- Retry Configuration ---
RETRYABLE_EXCEPTIONS_TENACITY = ( GoogleAPIError, ConnectionError, TimeoutError, ServiceUnavailable, BadRequest )
def retry_with_backoff_tenacity(retries=3, backoff_in_seconds=1, max_wait=10, retry_exceptions=RETRYABLE_EXCEPTIONS_TENACITY):
    """Returns a tenacity retry decorator with exponential backoff."""
    # (Implementation as before)
    if not isinstance(retry_exceptions, tuple): retry_exceptions = (retry_exceptions,)
    return tenacity_retry(retry=tenacity_if_exception_type(retry_exceptions), stop=stop_after_attempt(retries), wait=wait_exponential(multiplier=backoff_in_seconds, min=1, max=max_wait), before_sleep=lambda rs: logger.warning(f"Retrying {rs.fn.__name__} ({rs.attempt_number})"), reraise=True)

# --- GCP Clients ---
bq_client: Optional[bigquery.Client] = None
pubsub_publisher: Optional[pubsub_v1.PublisherClient] = None
CATALOG_TOPIC_PATH: Optional[str] = None

# --- Client Initialization (SHOULD BE IN SHARED UTILS) ---
def initialize_clients(max_retries=3):
    """Initializes clients needed for this service."""
    global bq_client, pubsub_publisher, CATALOG_TOPIC_PATH
    if bq_client and (pubsub_publisher or not CATALOG_PUBSUB_TOPIC_ID): return
    logger.info("Attempting to initialize GCP clients (BigQuery, Pub/Sub)...")
    retry_count=0
    while retry_count < max_retries:
        try:
            if not bq_client: bq_client=bigquery.Client(project=PROJECT_ID); bq_client.list_datasets(max_results=1); logger.info("BigQuery client initialized.")
            if CATALOG_PUBSUB_TOPIC_ID and not pubsub_publisher:
                try: pubsub_publisher=pubsub_v1.PublisherClient(); CATALOG_TOPIC_PATH=pubsub_publisher.topic_path(PROJECT_ID,CATALOG_PUBSUB_TOPIC_ID); pubsub_publisher.get_topic(request={"topic":CATALOG_TOPIC_PATH}); logger.info(f"Pub/Sub publisher initialized for topic: {CATALOG_TOPIC_PATH}")
                except NotFound: logger.error(f"Pub/Sub topic '{CATALOG_PUBSUB_TOPIC_ID}' not found. Triggering disabled."); pubsub_publisher=None; CATALOG_TOPIC_PATH=None
                except Exception as pubsub_err: logger.error(f"Pub/Sub init failed: {pubsub_err}", exc_info=True); pubsub_publisher=None; CATALOG_TOPIC_PATH=None
            if bq_client: logger.info("Successfully initialized required GCP clients."); return
        except Exception as e:
            retry_count+=1; wait_time=2**retry_count; logger.warning(f"Client init error: {e}. Retry {retry_count}/{max_retries} in {wait_time}s", exc_info=False); bq_client=pubsub_publisher=None; CATALOG_TOPIC_PATH=None
            if retry_count >= max_retries: logger.error("Client init failed after retries."); raise RuntimeError("Client initialization failed")
            time.sleep(wait_time)

# =============================================================================
# == SHARED NAMING UTILITIES (Conceptually in pipeline_utils/naming.py) ==
# =============================================================================
def _sanitize_bq_name(name_part: str) -> str:
    """Removes invalid chars for BQ names, converts to lowercase."""
    if not name_part: return "unknown"
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name_part)
    sanitized = re.sub(r'_+', '_', sanitized).strip('_')
    return sanitized.lower() if sanitized else "sanitized_empty"

# get_staging_table_id is not needed by Cell 3, only by Cell 2

def get_cleaned_table_id(staging_table_id: str) -> Optional[str]:
    """Determines the target cleaned table name (clean_*) from stg_* table ID."""
    parts = staging_table_id.split('.')
    if len(parts) != 3 or not parts[2].startswith("stg_"):
        logger.error(f"Invalid staging table ID format for cleaning: {staging_table_id}")
        return None
    project, dataset, stg_table_name = parts
    cleaned_table_name = stg_table_name.replace("stg_", "clean_", 1)
    # Validation for the generated name
    if not re.match(r"^[a-zA-Z0-9_]+$", cleaned_table_name):
        logger.error(f"Generated cleaned table name contains invalid characters: '{cleaned_table_name}' from '{staging_table_id}'")
        return None
    if len(cleaned_table_name) > 1024: cleaned_table_name = cleaned_table_name[:1024]

    full_cleaned_id = f"{project}.{dataset}.{cleaned_table_name}"
    logger.info(f"Determined cleaned table ID: {full_cleaned_id}")
    return full_cleaned_id

def extract_source_from_table_name(table_name_or_id: str) -> Optional[str]:
    """Extracts source (e.g., 'facebook') from stg_ or clean_ table name/ID."""
    table_name_only = table_name_or_id.split('.')[-1]
    # Updated regex to handle potential 'mycocoons' prefix if Cell 2 included it (it shouldn't now)
    # And make source capture non-greedy and specific
    match = re.match(r"(?:stg|clean)_([a-z0-9]+?)_", table_name_only, re.IGNORECASE) # Match lowercase source
    return match.group(1).lower() if match else None
# =============================================================================


# --- Cleaning Functions (SHOULD BE IN SEPARATE MODULES/PLUGINS) ---
# Placeholder implementations - replace with your actual cleaning logic
def clean_facebook_ads_data(df: pd.DataFrame) -> pd.DataFrame: logger.info("Cleaning Facebook data..."); return df
def clean_google_analytics_data(df: pd.DataFrame, table_type: str) -> pd.DataFrame: logger.info(f"Cleaning GA data ({table_type})..."); return df
def clean_shopify_data(df: pd.DataFrame) -> pd.DataFrame: logger.info("Cleaning Shopify data..."); return df
def clean_ga4custom_data(df: pd.DataFrame) -> pd.DataFrame: logger.info("Cleaning GA4 data..."); return df
def clean_google_ads_data(df: pd.DataFrame) -> pd.DataFrame: logger.info("Cleaning Google Ads data..."); return df
def clean_generic_data(df: pd.DataFrame, table_id: str) -> pd.DataFrame: logger.warning(f"Using generic cleaner for {table_id}"); return df
# --- End Cleaning Functions Placeholder ---


# --- Schema Validation and Loading ---
@retry_with_backoff_tenacity()
def validate_and_update_schema(df: pd.DataFrame, table_ref: bigquery.TableReference, client: bigquery.Client) -> None:
    """Validates DataFrame schema against BigQuery table, updates table schema if needed."""
    logger.debug(f"Validating schema for table {table_ref.table_id}")
    try:
        # *** FIX: Indent code block under try: ***
        table = client.get_table(table_ref)
        current_schema_map = {field.name: field for field in table.schema}
        original_schema = table.schema[:]
        needs_update = False
        new_schema_fields = []
        for column_name in df.columns:
            if column_name not in current_schema_map:
                needs_update = True
                field_type = "STRING"; col_dtype = df[column_name].dtype
                if pd.api.types.is_integer_dtype(col_dtype): field_type = "INT64"
                elif pd.api.types.is_float_dtype(col_dtype): field_type = "FLOAT64"
                elif pd.api.types.is_datetime64_any_dtype(col_dtype): field_type = "TIMESTAMP"
                elif pd.api.types.is_bool_dtype(col_dtype): field_type = "BOOL"
                elif pd.api.types.is_object_dtype(col_dtype):
                    try: first_valid = df[column_name].dropna().iloc[0]
                    except IndexError: pass
                    else:
                        if isinstance(first_valid, (dict, list)): field_type = "JSON"
                        elif isinstance(first_valid, str) and first_valid.strip().startswith(("{", "[")): field_type = "JSON"
                new_field = bigquery.SchemaField(name=column_name, field_type=field_type, mode="NULLABLE"); table.schema.append(new_field); new_schema_fields.append(f"{column_name} ({field_type})")
        if needs_update:
            logger.info(f"Schema update for {table_ref.table_id}. Adding: {', '.join(new_schema_fields)}. Updating...")
            try: client.update_table(table, ["schema"]); logger.info(f"Schema updated for {table_ref.table_id}")
            except Exception as update_err: logger.error(f"Schema update failed for {table_ref.table_id}: {update_err}", exc_info=True); table.schema = original_schema; raise
        else: logger.debug(f"Schema OK for {table_ref.table_id}.")
    except NotFound:
        logger.info(f"Cleaned table {table_ref.table_id} not found. Schema created on load.")
    except Exception as e: logger.error(f"Schema validation error for {table_ref.table_id}: {e}", exc_info=True); raise

@retry_with_backoff_tenacity()
def load_dataframe_to_bq(df: pd.DataFrame, table_ref: bigquery.TableReference, client: bigquery.Client) -> None:
    """Loads a Pandas DataFrame to the specified BigQuery table."""
    # (Implementation remains the same as v3)
    logger.info(f"Loading {len(df)} rows into {table_ref.table_id}...");
    for col in df.select_dtypes(include=['datetime64[ns]','datetime64[ns, UTC]']).columns:
        try:
            if df[col].dt.tz is None: df[col]=df[col].dt.tz_localize('UTC',ambiguous='infer',nonexistent='shift_forward')
            elif str(df[col].dt.tz)!='UTC': df[col]=df[col].dt.tz_convert('UTC')
        except Exception as tz_err: logger.warning(f"TZ error {col}: {tz_err}.")
    job_config=bigquery.LoadJobConfig(schema=client.schema_from_dataframe(df),write_disposition="WRITE_TRUNCATE",create_disposition="CREATE_IF_NEEDED")
    try: load_job=client.load_table_from_dataframe(df,table_ref,job_config=job_config); load_job.result(timeout=600)
    except Exception as e: logger.error(f"Load df->BQ failed {table_ref.table_id}: {e}",exc_info=True); raise
    if load_job.errors: error_str="; ".join([f"{err.get('reason','?')}:{err.get('message','?')}" for err in load_job.errors]); logger.error(f"Load job {table_ref.table_id} errors: {error_str}"); raise GoogleAPIError(f"Load job failed: {error_str}")
    else: dest_table=client.get_table(table_ref); logger.info(f"Loaded {dest_table.num_rows} rows to {table_ref.table_id}.")


# --- Helper Functions ---
CLEANER_MAPPING: Dict[str, Callable] = {
    'facebook': clean_facebook_ads_data,
    'gaanalytics': clean_google_analytics_data,
    'shopify': clean_shopify_data,
    'ga4custom': clean_ga4custom_data,
    'google': clean_google_ads_data,
}

def get_cleaner_function(stg_table_id: str) -> Callable:
    """Selects the appropriate cleaning function based on stg table ID."""
    # *** Use shared naming function ***
    source_name = extract_source_from_table_name(stg_table_id)
    if source_name and source_name in CLEANER_MAPPING:
        func = CLEANER_MAPPING[source_name]; sig = inspect.signature(func)
        # Pass full table ID as context if function expects 'table_type'
        if 'table_type' in sig.parameters: return lambda df: func(df, stg_table_id)
        else: return lambda df: func(df)
    else:
        logger.warning(f"No specific cleaner for source '{source_name}' from {stg_table_id}. Using generic.")
        return lambda df: clean_generic_data(df, stg_table_id)

# --- Triggering Next Step ---
@retry_with_backoff_tenacity(retries=2)
def trigger_catalog_job(cleaned_table_id: str, stg_table_id: str):
    """Publishes a message to trigger the cataloging job (Cell 4)."""
    if not pubsub_publisher or not CATALOG_TOPIC_PATH:
        logger.warning(f"Pub/Sub catalog trigger disabled for {CATALOG_PUBSUB_TOPIC_ID}. Skipping.")
        return

    message_data = { "cleaned_table_id": cleaned_table_id, "source_stg_table": stg_table_id, "trigger_timestamp": datetime.utcnow().isoformat() }
    message_body = json.dumps(message_data).encode("utf-8")
    try:
        future = pubsub_publisher.publish(CATALOG_TOPIC_PATH, message_body)
        future.result(timeout=30)
        logger.info(f"Published catalog request for {cleaned_table_id} to {CATALOG_PUBSUB_TOPIC_ID}.")
    except Exception as e:
        logger.error(f"Failed catalog publish for {cleaned_table_id}: {e}", exc_info=True)
        raise

# --- Main Handler Function (for Cloud Function/Run) ---
def handle_cleaning_request(event: Dict[str, Any], context: Any):
    """Cloud Function/Run entry point triggered by Pub/Sub message from Cell 2."""
    start_time=time.time(); stg_table_id=None; cleaned_table_id=None; status="FAILED"; error_message="Processing started but did not complete."
    try:
        initialize_clients()
    except Exception as init_err:
        logger.critical(f"Client init failed: {init_err}", exc_info=True); return

    try:
        if 'data' not in event: raise ValueError("No 'data' field")
        message_data=json.loads(base64.b64decode(event['data']).decode('utf-8'))
        stg_table_id=message_data.get('target_table_id'); source_blob=message_data.get('source_blob_name','Unknown')
        if not stg_table_id: raise ValueError("Missing 'target_table_id'")
        logger.info(f"Received cleaning request for: {stg_table_id} (from: {source_blob})")

        # *** Use shared naming function ***
        cleaned_table_id=get_cleaned_table_id(stg_table_id);
        if not cleaned_table_id: raise ValueError(f"Cannot determine cleaned ID for {stg_table_id}")
        cleaned_table_ref=bigquery.TableReference.from_string(cleaned_table_id)

        logger.debug(f"Reading data from {stg_table_id}"); query=f"SELECT * FROM `{stg_table_id}`"
        try:
            query_job=bq_client.query(query, job_config=bigquery.QueryJobConfig(use_query_cache=False)); logger.info(f"Read job: {query_job.job_id}")
            # *** FIX: Corrected to_dataframe call ***
            df=query_job.to_dataframe()
        except NotFound as e: raise NotFound(f"Input staging table not found: {stg_table_id}") from e
        except Exception as query_err: logger.error(f"Query/fetch error for {stg_table_id}: {query_err}",exc_info=True); raise query_err
        logger.info(f"Read {len(df)} rows from {stg_table_id}")

        if df.empty: logger.warning(f"Staging table {stg_table_id} empty. Skipping."); status="SUCCESS"; error_message="Skipped (Source Empty)"; return

        logger.debug(f"Applying cleaning: {stg_table_id}"); cleaner_func=get_cleaner_function(stg_table_id); cleaned_df=cleaner_func(df.copy())

        if cleaned_df.empty: logger.warning(f"Empty df after cleaning {stg_table_id}. Skipping load."); status="SUCCESS"; error_message="Skipped (Empty After Clean)"; return

        validate_and_update_schema(cleaned_df, cleaned_table_ref, bq_client)
        load_dataframe_to_bq(cleaned_df, cleaned_table_ref, bq_client)

        status="SUCCESS"; error_message=None; logger.info(f"Successfully cleaned {stg_table_id} -> {cleaned_table_id}")

    except NotFound as e: status="FAILED"; error_message=f"Input table not found: {stg_table_id}. Error: {e}"; logger.error(error_message)
    except ValueError as e: status="FAILED"; error_message = f"Naming or Validation Error: {str(e)}"; logger.error(error_message)
    except Exception as e: status="FAILED"; error_message=f"Error cleaning {stg_table_id}: {str(e)}"; logger.error(error_message, exc_info=True)

    finally:
        if status=="SUCCESS" and cleaned_table_id and error_message is None:
            try: trigger_catalog_job(cleaned_table_id, stg_table_id)
            except Exception as trigger_err: logger.error(f"Clean OK {stg_table_id}, but failed catalog trigger: {trigger_err}", exc_info=True)
        end_time=time.time(); final_log_status=status if status else "FAILED"
        logger.info(f"Finished cleaning request for {stg_table_id or 'unknown table'} in {end_time - start_time:.2f} seconds. Status: {final_log_status}")

def generate_journey_sql_dyn(project: str, dataset: str, date_suffix: str, output_table: str):
    tables = list_clean_tables_for_date(dataset, date_suffix)
    if not tables:
        raise RuntimeError(f"No clean tables found for date {date_suffix}! Cannot build journey table.")

    sql_blocks = []
    for tbl in tables:
        # Facebook
        if "facebook" in tbl.lower():
            sql_blocks.append(f"""
                SELECT
                  COALESCE(CAST(ad_id AS STRING), 'unknown_fb') AS customer_id,
                  CAST(COALESCE(reporting_starts, date_start) AS TIMESTAMP) AS event_timestamp,
                  'facebook_ad_event' AS event_name,
                  'FacebookAds' AS source_system,
                  '{tbl}' AS source_table_suffix,
                  TO_JSON_STRING(STRUCT(campaign_id, ad_id, geo, spend, clicks, impressions, reach, frequency)) AS event_properties
                FROM `{project}.{dataset}.{tbl}`
                WHERE COALESCE(reporting_starts, date_start) IS NOT NULL
            """)
        # GA4
        elif "ga4" in tbl.lower():
            sql_blocks.append(f"""
                SELECT
                  COALESCE(CAST(user_pseudo_id AS STRING), unified_id, 'unknown_ga4') AS customer_id,
                  CAST(event_datetime AS TIMESTAMP) AS event_timestamp,
                  CAST(event_name AS STRING) AS event_name,
                  'GA4Custom' AS source_system,
                  '{tbl}' AS source_table_suffix,
                  TO_JSON_STRING(STRUCT(ga_session_id, stream_id, engagement_time_msec)) AS event_properties
                FROM `{project}.{dataset}.{tbl}`
                WHERE event_datetime IS NOT NULL
            """)
        # GA Analytics
        elif "gaanalytics" in tbl.lower():
            sql_blocks.append(f"""
                SELECT
                  COALESCE(CAST(ga_clientid AS STRING), CAST(ga_userid AS STRING), unified_id, 'unknown_ga3') AS customer_id,
                  CAST(COALESCE(session_date, date) AS TIMESTAMP) AS event_timestamp,
                  'gaanal_event' AS event_name,
                  'GAAnalytics' AS source_system,
                  '{tbl}' AS source_table_suffix,
                  TO_JSON_STRING(STRUCT(ga_sessionid, source, medium, campaign, page_path, hostname, transaction_id, item_revenue, item_quantity)) AS event_properties
                FROM `{project}.{dataset}.{tbl}`
                WHERE COALESCE(session_date, date) IS NOT NULL
            """)
        # Shopify
        elif "shopify" in tbl.lower():
            sql_blocks.append(f"""
                SELECT
                  COALESCE(CAST(customer_id AS STRING), 'unknown_shp') AS customer_id,
                  CAST(created_at AS TIMESTAMP) AS event_timestamp,
                  CASE
                    WHEN REGEXP_CONTAINS('{tbl}', 'ORDER') THEN 'shopify_order_created'
                    WHEN REGEXP_CONTAINS('{tbl}', 'CHECKOUT') THEN 'shopify_checkout_created'
                    ELSE 'shopify_generic_event'
                  END AS event_name,
                  'Shopify' AS source_system,
                  '{tbl}' AS source_table_suffix,
                  TO_JSON_STRING(STRUCT(order_id, checkout_id, cart_token, total_price, subtotal_price, total_discounts, financial_status, fulfillment_status)) AS event_properties
                FROM `{project}.{dataset}.{tbl}`
                WHERE created_at IS NOT NULL
            """)
        # Extend with other sources as you add them

    full_sql = f"""
    CREATE OR REPLACE TABLE `{project}.{dataset}.{output_table}_{date_suffix}` AS
    WITH SourceEvents AS (
        {" UNION ALL ".join(sql_blocks)}
    ), SequencedEvents AS (
        SELECT
            *,
            CAST(FARM_FINGERPRINT(
                customer_id || FORMAT_TIMESTAMP('%Y%m%d%H%M%S%f', event_timestamp) || event_name || source_system || source_table_suffix
            ) AS STRING) AS event_id,
            ROW_NUMBER() OVER (
                PARTITION BY customer_id
                ORDER BY event_timestamp ASC, source_system ASC, source_table_suffix ASC
            ) AS journey_step_seq
        FROM SourceEvents
        WHERE customer_id IS NOT NULL
          AND customer_id != 'unknown'
          AND customer_id != ''
          AND event_timestamp IS NOT NULL
    )
    SELECT
        customer_id,
        event_timestamp,
        event_id,
        event_name,
        source_system,
        source_table_suffix,
        event_properties,
        journey_step_seq
    FROM SequencedEvents;
    """
    return full_sql
# --- Entry point for Cloud Functions/Run ---
# See previous response for example Flask/functions_framework wrappers
# If deploying, ensure the entry point calls handle_cleaning_request appropriately.
