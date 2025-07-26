```python

!pip install -q google‑cloud-bigquery google‑cloud-storage \
        google‑cloud-pubsub google‑cloud-datacatalog google-api-core \
        google-auth tenacity tqdm pandas nest_asyncio`

```

    /bin/bash: -c: line 1: unexpected EOF while looking for matching ``'
    /bin/bash: -c: line 2: syntax error: unexpected end of file



```python
# Cell 1: Environment Setup & Discovery (Batch Inserts/Updates, Decoupled Trigger Status - v6 FINAL)
# Cell 1: Environment Setup & Discovery (Batch Inserts/Updates, Decoupled Trigger Status - v6 FINAL)
import logging
import os
import time
import random
import json
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple, Set
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import bigquery, storage, pubsub_v1
from google.api_core.exceptions import NotFound, Conflict, GoogleAPIError, AlreadyExists
from google.cloud import exceptions # Import base exceptions for retry
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s'
)
logger = logging.getLogger(__name__)
logging.getLogger('google.api_core').setLevel(logging.WARNING)
logging.getLogger('google.auth').setLevel(logging.WARNING)
logging.getLogger('google.cloud').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)


# --- Configuration ---
PROJECT_ID = os.environ.get('PROJECT_ID', "spry-bus-425315-p6")
BUCKET_NAME = os.environ.get('BUCKET_NAME', "mycocoons")
DATASET_ID = os.environ.get('DATASET_ID', "mycocoons")
LOCATION = os.environ.get('LOCATION', "US") # BQ Dataset Location

# Manifest table details
MANIFEST_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.pipeline_manifest"
MANIFEST_SCHEMA = [
    bigquery.SchemaField("blob_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("bucket_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("size_bytes", "INTEGER"),
    bigquery.SchemaField("content_type", "STRING"),
    bigquery.SchemaField("md5_hash", "STRING"),
    bigquery.SchemaField("crc32c", "STRING"),
    bigquery.SchemaField("generation", "INTEGER"),
    bigquery.SchemaField("discovered_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("processed_at", "TIMESTAMP"),
    bigquery.SchemaField("processing_status", "STRING", mode="REQUIRED"), # PENDING, PROCESSING, SUCCESS, FAILED
    bigquery.SchemaField("target_table", "STRING"), # BQ table where data was loaded (stg_*)
    bigquery.SchemaField("error_message", "STRING"),
    bigquery.SchemaField("retry_count", "INTEGER", mode="REQUIRED", default_value_expression="0"),
]

# --- Trigger Configuration ---
USE_PUBSUB = os.environ.get('USE_PUBSUB', 'true').lower() == 'true' # Default to TRUE
PUBSUB_TOPIC_ID = os.environ.get('PUBSUB_PARSING_TOPIC', 'etl-parsing-requests')
USE_CLOUD_TASKS = False # Explicitly disabled

# --- Data Sources ---
DATA_SOURCES = {
    'Facebook':    {'prefix': 'mycocoons/Facebook/', 'format': 'csv'},
    'GA4Custom':   {'prefix': 'mycocoons/GA4Custom/', 'format': 'csv'},
    'GAAnalytics': {'prefix': 'mycocoons/GAAnalytics/', 'format': 'csv'},
    'Google':      {'prefix': 'mycocoons/Google/', 'format': 'csv'},
    'Shopify':     {'prefix': 'mycocoons/Shopify/', 'format': 'csv'},
}

# --- Performance Tuning ---
DISCOVERY_BATCH_SIZE = 2000
TRIGGER_MAX_WORKERS = 20
DISCOVERY_MAX_WORKERS = len(DATA_SOURCES)

# --- Retry Configuration ---
RETRYABLE_EXCEPTIONS = (
    GoogleAPIError, ConnectionError, TimeoutError,
    exceptions.ServerError, exceptions.TooManyRequests, exceptions.BadRequest
)
retry_with_backoff = retry(
    retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1.5, min=2, max=15),
    before_sleep=lambda retry_state: logger.warning(
        f"Retrying {retry_state.fn.__name__} due to {retry_state.outcome.exception()} "
        f"(attempt {retry_state.attempt_number})"
    ),
    reraise=True
)

# --- GCP Clients ---
bq_client = None
storage_client = None
pubsub_publisher = None
PUBSUB_TOPIC_PATH = None

def initialize_gcp_clients():
    """Initializes GCP clients needed for this cell."""
    global bq_client, storage_client, pubsub_publisher, PUBSUB_TOPIC_PATH
    if bq_client and storage_client and (pubsub_publisher or not USE_PUBSUB): return

    try:
        if not bq_client:
            bq_client = bigquery.Client(project=PROJECT_ID)
            logger.info("BigQuery client initialized successfully.")
        if not storage_client:
            storage_client = storage.Client(project=PROJECT_ID)
            logger.info("Storage client initialized successfully.")
        if USE_PUBSUB and not pubsub_publisher:
            try:
                pubsub_publisher = pubsub_v1.PublisherClient()
                PUBSUB_TOPIC_PATH = pubsub_publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC_ID)
                pubsub_publisher.get_topic(request={"topic": PUBSUB_TOPIC_PATH}) # Check existence
                logger.info(f"Pub/Sub publisher initialized for topic: {PUBSUB_TOPIC_PATH}")
            except NotFound:
                logger.error(f"Pub/Sub topic '{PUBSUB_TOPIC_ID}' not found. Triggering will be disabled.")
                pubsub_publisher = None
            except Exception as e:
                logger.error(f"Failed to initialize Pub/Sub publisher: {e}", exc_info=True)
                pubsub_publisher = None
    except Exception as e:
        logger.error(f"Failed to initialize GCP clients: {e}", exc_info=True)
        bq_client = storage_client = pubsub_publisher = None
        raise RuntimeError("Client initialization failed")

# --- Service Functions (Manifest, Discovery, Triggering) ---

@retry_with_backoff
def create_dataset_if_not_exists() -> None:
    """Creates the BigQuery dataset if it doesn't exist. Idempotent."""
    if not bq_client: raise RuntimeError("BigQuery client not initialized.")
    dataset_ref = bq_client.dataset(DATASET_ID)
    try:
        bq_client.get_dataset(dataset_ref)
        logger.info(f"Dataset {PROJECT_ID}.{DATASET_ID} already exists.")
    except NotFound:
        logger.info(f"Dataset {PROJECT_ID}.{DATASET_ID} not found. Creating...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = LOCATION
        bq_client.create_dataset(dataset, exists_ok=True)
        logger.info(f"Dataset {PROJECT_ID}.{DATASET_ID} created or already exists in {LOCATION}.")

@retry_with_backoff
def create_manifest_table_if_not_exists() -> None:
    """Creates the manifest table in BigQuery if it doesn't exist. Idempotent."""
    if not bq_client: raise RuntimeError("BigQuery client not initialized.")
    try:
        bq_client.get_table(MANIFEST_TABLE_ID)
        logger.info(f"Manifest table {MANIFEST_TABLE_ID} already exists.")
    except NotFound:
        logger.info(f"Manifest table {MANIFEST_TABLE_ID} not found. Creating...")
        table = bigquery.Table(MANIFEST_TABLE_ID, schema=MANIFEST_SCHEMA)
        table.time_partitioning = bigquery.TimePartitioning(field="discovered_at", type_="DAY")
        table.clustering_fields = ["processing_status", "bucket_name"]
        try:
            bq_client.create_table(table)
            logger.info(f"Manifest table {MANIFEST_TABLE_ID} created.")
        except Conflict:
             logger.info(f"Manifest table {MANIFEST_TABLE_ID} was created by another process.")
        except Exception as e:
            logger.error(f"Failed to create manifest table {MANIFEST_TABLE_ID}: {e}", exc_info=True)
            raise
    except Exception as e:
        logger.error(f"Error checking/creating manifest table {MANIFEST_TABLE_ID}: {e}", exc_info=True)
        raise

@retry_with_backoff
def batch_check_manifest(blob_names: List[str]) -> Dict[str, Dict[str, Any]]:
    """Checks a batch of blob names against the manifest table."""
    if not bq_client: raise RuntimeError("BigQuery client not initialized.")
    if not blob_names: return {}
    logger.debug(f"Batch checking manifest for {len(blob_names)} blobs.")
    query = f"""
        SELECT blob_name, processing_status, retry_count
        FROM `{MANIFEST_TABLE_ID}`
        WHERE blob_name IN UNNEST(@blob_names)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY blob_name ORDER BY discovered_at DESC) = 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("blob_names", "STRING", blob_names)]
    )
    try:
        results = bq_client.query(query, job_config=job_config).result(timeout=180)
        manifest_status = {row.blob_name: dict(row.items()) for row in results}
        logger.debug(f"Found {len(manifest_status)} existing manifest entries for batch.")
        return manifest_status
    except Exception as e:
        logger.error(f"Error batch checking manifest: {e}", exc_info=True)
        raise

@retry_with_backoff
def batch_insert_pending_manifest(rows_to_insert: List[Dict[str, Any]]) -> None:
    """Batch inserts new PENDING rows into the manifest table via streaming insert."""
    if not bq_client: raise RuntimeError("BigQuery client not initialized.")
    if not rows_to_insert: return
    logger.info(f"Batch inserting {len(rows_to_insert)} new PENDING manifest entries...")
    try:
        # Use insert_rows_json - set skip_invalid_rows=False to fail fast
        errors = bq_client.insert_rows_json(MANIFEST_TABLE_ID, rows_to_insert, skip_invalid_rows=False)
        if errors: # Should not be reached if skip_invalid_rows=False and error occurs
            error_count = sum(len(e.get('errors', [])) for e in errors)
            logger.error(f"Encountered {error_count} errors during batch manifest insert (unexpected with skip_invalid_rows=False).")
            raise GoogleAPIError(f"{error_count} errors during batch manifest insert.")
        else:
            logger.info(f"Successfully batch inserted {len(rows_to_insert)} manifest entries.")
    except Exception as e:
        logger.error(f"Error batch inserting manifest rows: {e}", exc_info=True)
        raise # Reraise to allow retry decorator to work

@retry_with_backoff
def batch_update_retries_to_pending(blob_names: List[str]) -> None:
    """Batch updates manifest rows from FAILED/Other to PENDING for retry using UPDATE DML."""
    if not bq_client: raise RuntimeError("BigQuery client not initialized.")
    if not blob_names: return
    logger.info(f"Batch updating {len(blob_names)} FAILED/unexpected blobs to PENDING for retry...")
    update_sql = f"""
        UPDATE `{MANIFEST_TABLE_ID}`
        SET
            processing_status = 'PENDING',
            error_message = 'Resetting for retry',
            processed_at = NULL
        WHERE blob_name IN UNNEST(@blob_names)
        AND processing_status NOT IN ('SUCCESS', 'PROCESSING', 'PENDING')
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("blob_names", "STRING", blob_names)]
    )
    try:
        update_job = bq_client.query(update_sql, job_config=job_config)
        update_job.result(timeout=180)
        # Log affected rows if needed: update_job.num_dml_affected_rows
        logger.info(f"Batch update to PENDING complete for {len(blob_names)} candidate blobs.")
    except Exception as e:
        logger.error(f"Error batch updating retries to PENDING: {e}", exc_info=True)
        raise

# @retry_with_backoff # Retries handled internally by batch check/insert/update
def discover_new_blobs(bucket_name: str, prefix: str, batch_size: int = DISCOVERY_BATCH_SIZE) -> List[storage.Blob]:
    """Discovers blobs, checks manifest in batches, inserts/updates manifest."""
    if not storage_client: raise RuntimeError("Storage client not initialized.")

    blobs_to_process: List[storage.Blob] = []
    processed_in_this_run: Set[str] = set()
    total_scanned = 0
    total_skipped = 0
    total_new_pending = 0
    total_retry_pending = 0
    max_retries = 3
    now_ts_utc = datetime.utcnow()

    try:
        logger.info(f"Discovering blobs in gs://{bucket_name}/{prefix} (Batch size: {batch_size})")
        blob_pages = storage_client.list_blobs(bucket_name, prefix=prefix, page_size=batch_size).pages

        for page_num, page in enumerate(blob_pages):
            current_batch_blobs: List[storage.Blob] = []
            current_batch_blob_names: List[str] = []

            logger.debug(f"Processing GCS page {page_num + 1} for prefix '{prefix}'...")
            for blob in page:
                total_scanned += 1
                if blob.name.endswith('/') or blob.size == 0:
                    total_skipped += 1
                    continue
                if blob.name not in processed_in_this_run:
                    current_batch_blobs.append(blob)
                    current_batch_blob_names.append(blob.name)

            if not current_batch_blob_names:
                logger.debug(f"Page {page_num + 1} contained no new valid blobs.")
                continue

            # Batch check manifest
            try:
                manifest_status = batch_check_manifest(current_batch_blob_names)
            except Exception as batch_check_err:
                logger.error(f"Failed batch manifest check for page {page_num + 1}. Skipping blobs in this batch. Error: {batch_check_err}", exc_info=True)
                total_skipped += len(current_batch_blobs)
                continue

            # Process blobs based on manifest status
            new_rows_to_insert: List[Dict[str, Any]] = []
            blobs_needing_retry_update: List[str] = [] # Store names for batch update

            for blob in current_batch_blobs:
                status_info = manifest_status.get(blob.name)
                needs_processing = False # Flag to add to trigger list

                if status_info is None:
                    # *** NEW BLOB *** -> Prepare for Batch Insert
                    needs_processing = True
                    total_new_pending += 1
                    new_rows_to_insert.append({
                        "blob_name": blob.name, "bucket_name": blob.bucket.name,
                        "size_bytes": blob.size, "content_type": blob.content_type,
                        "md5_hash": blob.md5_hash, "crc32c": blob.crc32c,
                        "generation": blob.generation, "discovered_at": now_ts_utc.isoformat(),
                        "processed_at": None, "processing_status": "PENDING",
                        "target_table": None, "error_message": None, "retry_count": 0
                    })

                elif status_info['processing_status'] == "PENDING":
                     # *** ALREADY PENDING *** -> Add to trigger list
                     needs_processing = True
                     logger.debug(f"Blob {blob.name} already PENDING. Adding to trigger list.")

                elif status_info['processing_status'] == "FAILED":
                    # *** FAILED BLOB - Check for Retry *** -> Prepare for Batch Update
                    current_retries = status_info.get('retry_count', 0)
                    if current_retries < max_retries:
                        needs_processing = True
                        total_retry_pending += 1
                        blobs_needing_retry_update.append(blob.name) # Add name for BATCH update
                    else:
                        total_skipped += 1 # Exceeded retries

                elif status_info['processing_status'] in ["SUCCESS", "PROCESSING"]:
                    # *** Already Done or In Progress ***
                    total_skipped += 1
                else:
                    # *** Truly Unexpected Status *** -> Prepare for Batch Update
                    needs_processing = True
                    logger.warning(f"Blob {blob.name} has unexpected status '{status_info['processing_status']}'. Marking for retry update.")
                    total_retry_pending += 1
                    blobs_needing_retry_update.append(blob.name) # Add name for BATCH update

                # Add to list for triggering if it needs processing
                if needs_processing and blob.name not in processed_in_this_run:
                     blobs_to_process.append(blob)

                processed_in_this_run.add(blob.name) # Mark handled in this run

            # --- Perform Batch Operations for the Page ---
            # 1. Batch insert NEW blobs
            if new_rows_to_insert:
                try:
                    batch_insert_pending_manifest(new_rows_to_insert)
                except Exception as batch_insert_err:
                    logger.error(f"Failed batch manifest insert for page {page_num + 1}. Blobs might be reprocessed. Error: {batch_insert_err}", exc_info=True)
                    new_blob_names_in_batch = {row['blob_name'] for row in new_rows_to_insert}
                    blobs_to_process = [b for b in blobs_to_process if b.name not in new_blob_names_in_batch]
                    total_new_pending -= len(new_rows_to_insert)

            # 2. Batch update FAILED/OTHER blobs to PENDING for retry
            if blobs_needing_retry_update:
                try:
                    batch_update_retries_to_pending(blobs_needing_retry_update)
                except Exception as batch_update_err:
                    logger.error(f"Failed batch manifest update for retries. Some blobs may not be retried. Error: {batch_update_err}", exc_info=True)
                    blobs_to_process = [b for b in blobs_to_process if b.name not in blobs_needing_retry_update]
                    total_retry_pending -= len(blobs_needing_retry_update)
            # --- End Batch Operations ---

            logger.debug(f"Finished processing GCS page {page_num + 1}.")

        logger.info(f"Discovery complete for prefix '{prefix}'. Scanned: {total_scanned}, Skipped: {total_skipped}. Identified: {total_new_pending} new, {total_retry_pending} for retry.")
        return blobs_to_process
    except Exception as e:
        logger.error(f"Error during blob discovery for prefix '{prefix}': {e}", exc_info=True)
        raise

# @retry_with_backoff # Retry handled within publish call if needed
def trigger_parsing_job(blob: storage.Blob) -> bool:
    """Publishes trigger message via Pub/Sub (if enabled). Does NOT update manifest."""
    if not blob: logger.error("Cannot trigger parsing job for invalid blob object."); return False

    blob_name = blob.name
    bucket_name = blob.bucket.name
    message_data = {
        "blob_name": blob_name, "bucket_name": bucket_name,
        "trigger_timestamp": datetime.utcnow().isoformat(),
        "generation": blob.generation, "size_bytes": blob.size,
    }
    message_body = json.dumps(message_data).encode("utf-8")

    if USE_PUBSUB and pubsub_publisher and PUBSUB_TOPIC_PATH:
        try:
            future = pubsub_publisher.publish(PUBSUB_TOPIC_PATH, message_body)
            future.result(timeout=30)
            logger.debug(f"Published parsing request for {blob_name} to Pub/Sub.")
            return True # Indicate publish success
        except Exception as e:
            logger.error(f"Failed to publish to Pub/Sub for {blob_name}: {e}", exc_info=False)
            # Manifest update to FAILED should happen elsewhere if trigger fails consistently
            return False # Indicate publish failure
    else:
        reason = "No trigger mechanism (Pub/Sub) configured or enabled/available."
        logger.warning(f"No trigger sent for {blob_name}. Reason: {reason}")
        # Don't mark as FAILED here, just report no trigger sent
        return False # Indicate trigger was not sent

# --- Main Workflow ---
def run_discovery_and_trigger():
    """Main function: parallel discovery, batch manifest, parallel triggering."""
    logger.info("Initializing GCP clients...")
    try:
        initialize_gcp_clients()
    except Exception as init_err:
         logger.critical(f"Client initialization failed: {init_err}. Exiting.")
         return

    if not all([bq_client, storage_client]):
        logger.error("Essential GCP clients (BigQuery, Storage) not initialized. Exiting.")
        return
    if USE_PUBSUB and not pubsub_publisher:
         logger.warning("Pub/Sub is enabled but client failed to initialize. Triggering will not occur.")

    start_time = time.time()
    logger.info("Starting Discovery and Trigger process...")

    try:
        logger.info("Ensuring BigQuery dataset and manifest table exist...")
        create_dataset_if_not_exists()
        create_manifest_table_if_not_exists()
        logger.info("BigQuery setup verified.")

        all_pending_blobs: List[storage.Blob] = []
        discovery_futures = {}
        # --- Parallel Discovery ---
        logger.info(f"Starting parallel blob discovery across {len(DATA_SOURCES)} sources (Workers: {DISCOVERY_MAX_WORKERS})...")
        with ThreadPoolExecutor(max_workers=DISCOVERY_MAX_WORKERS, thread_name_prefix="discovery_worker") as executor:
            for source, config in DATA_SOURCES.items():
                prefix = config.get('prefix', source + '/')
                if not prefix.endswith('/'): prefix += '/'
                future = executor.submit(discover_new_blobs, BUCKET_NAME, prefix, DISCOVERY_BATCH_SIZE)
                discovery_futures[future] = source

            for future in as_completed(discovery_futures):
                source = discovery_futures[future]
                try:
                    pending_blobs_for_source = future.result()
                    all_pending_blobs.extend(pending_blobs_for_source)
                    logger.info(f"Finished discovery for source '{source}', identified {len(pending_blobs_for_source)} blobs for processing.")
                except Exception as e:
                    logger.error(f"Parallel discovery failed for source '{source}': {e}", exc_info=True)
        # --- End Parallel Discovery ---

        # Deduplicate blobs_to_process list just in case
        unique_pending_blobs = list({blob.name: blob for blob in all_pending_blobs}.values())
        logger.info(f"Total unique blobs identified for processing across all sources: {len(unique_pending_blobs)}")


        # --- Parallel Triggering ---
        if unique_pending_blobs:
            if not pubsub_publisher and USE_PUBSUB:
                 logger.error("Pub/Sub triggering enabled but publisher not available. Cannot trigger jobs.")
                 # Blobs remain PENDING in manifest
            elif not USE_PUBSUB:
                 logger.warning("No trigger mechanism enabled (USE_PUBSUB=false). Blobs will remain PENDING.")
            else:
                logger.info(f"--- Triggering parsing for {len(unique_pending_blobs)} blobs in parallel (Workers: {TRIGGER_MAX_WORKERS}) ---")
                trigger_success_count = 0
                trigger_failed_count = 0
                trigger_futures = {}
                with ThreadPoolExecutor(max_workers=TRIGGER_MAX_WORKERS, thread_name_prefix="trigger_worker") as executor:
                    for blob in unique_pending_blobs:
                        future = executor.submit(trigger_parsing_job, blob)
                        trigger_futures[future] = blob.name

                    processed_triggers = 0
                    for future in as_completed(trigger_futures):
                        blob_name = trigger_futures[future]
                        processed_triggers += 1
                        try:
                            success = future.result() # trigger_parsing_job returns True/False
                            if success:
                                trigger_success_count += 1
                            else:
                                trigger_failed_count += 1
                        except Exception as e:
                            logger.error(f"Triggering future failed unexpectedly for blob {blob_name}: {e}", exc_info=True)
                            trigger_failed_count += 1
                        if processed_triggers % 200 == 0 or processed_triggers == len(unique_pending_blobs): # Log progress more often
                             logger.info(f"Triggering progress: {processed_triggers}/{len(unique_pending_blobs)}...")

                logger.info(f"Parallel triggering attempts complete. Success (Published): {trigger_success_count}, Failed/Skipped: {trigger_failed_count}")
                if trigger_failed_count > 0:
                     logger.warning(f"{trigger_failed_count} blobs failed to trigger. They remain PENDING or marked FAILED in manifest.")
        else:
            logger.info("No new/retryable blobs found to trigger parsing.")
        # --- End Parallel Triggering ---

        end_time = time.time()
        logger.info(f"Discovery and Trigger process finished in {end_time - start_time:.2f} seconds.")

    except Exception as e:
        logger.critical(f"A critical error occurred in the main discovery process: {e}", exc_info=True)

# --- Execution ---
if __name__ == "__main__":
    logger.info("Starting ETL Discovery & Intake (Cell 1 - Batch Inserts/Updates v6 FINAL)...")
    run_discovery_and_trigger()
    logger.info("ETL Discovery & Intake (Cell 1 - Batch Inserts/Updates v6 FINAL) finished.")

```

    2025-04-26 17:41:51,812 - INFO - 159189309:488 - Starting ETL Discovery & Intake (Cell 1 - Batch Inserts/Updates v6 FINAL)...
    2025-04-26 17:41:51,813 - INFO - 159189309:392 - Initializing GCP clients...
    2025-04-26 17:41:51,838 - INFO - 159189309:102 - BigQuery client initialized successfully.
    2025-04-26 17:41:51,844 - INFO - 159189309:105 - Storage client initialized successfully.
    2025-04-26 17:41:52,017 - INFO - 159189309:111 - Pub/Sub publisher initialized for topic: projects/spry-bus-425315-p6/topics/etl-parsing-requests
    2025-04-26 17:41:52,018 - INFO - 159189309:406 - Starting Discovery and Trigger process...
    2025-04-26 17:41:52,019 - INFO - 159189309:409 - Ensuring BigQuery dataset and manifest table exist...
    2025-04-26 17:41:52,219 - INFO - 159189309:132 - Dataset spry-bus-425315-p6.mycocoons already exists.
    2025-04-26 17:41:52,413 - INFO - 159189309:146 - Manifest table spry-bus-425315-p6.mycocoons.pipeline_manifest already exists.
    2025-04-26 17:41:52,414 - INFO - 159189309:412 - BigQuery setup verified.
    2025-04-26 17:41:52,415 - INFO - 159189309:417 - Starting parallel blob discovery across 5 sources (Workers: 5)...
    2025-04-26 17:41:52,416 - INFO - 159189309:249 - Discovering blobs in gs://mycocoons/mycocoons/Facebook/ (Batch size: 2000)
    2025-04-26 17:41:52,420 - INFO - 159189309:249 - Discovering blobs in gs://mycocoons/mycocoons/GA4Custom/ (Batch size: 2000)
    2025-04-26 17:41:52,425 - INFO - 159189309:249 - Discovering blobs in gs://mycocoons/mycocoons/GAAnalytics/ (Batch size: 2000)
    2025-04-26 17:41:52,429 - INFO - 159189309:249 - Discovering blobs in gs://mycocoons/mycocoons/Google/ (Batch size: 2000)
    2025-04-26 17:41:52,435 - INFO - 159189309:249 - Discovering blobs in gs://mycocoons/mycocoons/Shopify/ (Batch size: 2000)
    2025-04-26 17:41:54,404 - INFO - 159189309:353 - Discovery complete for prefix 'mycocoons/Shopify/'. Scanned: 1166, Skipped: 85. Identified: 0 new, 0 for retry.
    2025-04-26 17:41:54,426 - INFO - 159189309:430 - Finished discovery for source 'Shopify', identified 1081 blobs for processing.
    2025-04-26 17:41:56,019 - INFO - 159189309:353 - Discovery complete for prefix 'mycocoons/GA4Custom/'. Scanned: 2173, Skipped: 1035. Identified: 0 new, 0 for retry.
    2025-04-26 17:41:56,020 - INFO - 159189309:430 - Finished discovery for source 'GA4Custom', identified 1138 blobs for processing.
    2025-04-26 17:41:57,499 - INFO - 159189309:353 - Discovery complete for prefix 'mycocoons/Google/'. Scanned: 5850, Skipped: 5300. Identified: 0 new, 0 for retry.
    2025-04-26 17:41:57,502 - INFO - 159189309:430 - Finished discovery for source 'Google', identified 550 blobs for processing.
    2025-04-26 17:42:01,471 - INFO - 159189309:353 - Discovery complete for prefix 'mycocoons/GAAnalytics/'. Scanned: 8278, Skipped: 4125. Identified: 0 new, 0 for retry.
    2025-04-26 17:42:01,473 - INFO - 159189309:430 - Finished discovery for source 'GAAnalytics', identified 4153 blobs for processing.
    2025-04-26 17:42:02,200 - INFO - 159189309:353 - Discovery complete for prefix 'mycocoons/Facebook/'. Scanned: 9505, Skipped: 1727. Identified: 0 new, 0 for retry.
    2025-04-26 17:42:02,203 - INFO - 159189309:430 - Finished discovery for source 'Facebook', identified 7778 blobs for processing.
    2025-04-26 17:42:02,212 - INFO - 159189309:437 - Total unique blobs identified for processing across all sources: 14700
    2025-04-26 17:42:02,214 - INFO - 159189309:448 - --- Triggering parsing for 14700 blobs in parallel (Workers: 20) ---
    2025-04-26 17:42:03,148 - INFO - 159189309:471 - Triggering progress: 200/14700...
    2025-04-26 17:42:03,633 - INFO - 159189309:471 - Triggering progress: 400/14700...
    2025-04-26 17:42:04,060 - INFO - 159189309:471 - Triggering progress: 600/14700...
    2025-04-26 17:42:04,439 - INFO - 159189309:471 - Triggering progress: 800/14700...
    2025-04-26 17:42:04,805 - INFO - 159189309:471 - Triggering progress: 1000/14700...
    2025-04-26 17:42:05,178 - INFO - 159189309:471 - Triggering progress: 1200/14700...
    2025-04-26 17:42:05,505 - INFO - 159189309:471 - Triggering progress: 1400/14700...
    2025-04-26 17:42:05,766 - INFO - 159189309:471 - Triggering progress: 1600/14700...
    2025-04-26 17:42:06,051 - INFO - 159189309:471 - Triggering progress: 1800/14700...
    2025-04-26 17:42:06,401 - INFO - 159189309:471 - Triggering progress: 2000/14700...
    2025-04-26 17:42:06,631 - INFO - 159189309:471 - Triggering progress: 2200/14700...
    2025-04-26 17:42:06,964 - INFO - 159189309:471 - Triggering progress: 2400/14700...
    2025-04-26 17:42:07,224 - INFO - 159189309:471 - Triggering progress: 2600/14700...
    2025-04-26 17:42:07,482 - INFO - 159189309:471 - Triggering progress: 2800/14700...
    2025-04-26 17:42:07,696 - INFO - 159189309:471 - Triggering progress: 3000/14700...
    2025-04-26 17:42:07,904 - INFO - 159189309:471 - Triggering progress: 3200/14700...
    2025-04-26 17:42:08,131 - INFO - 159189309:471 - Triggering progress: 3400/14700...
    2025-04-26 17:42:08,347 - INFO - 159189309:471 - Triggering progress: 3600/14700...
    2025-04-26 17:42:08,600 - INFO - 159189309:471 - Triggering progress: 3800/14700...
    2025-04-26 17:42:08,825 - INFO - 159189309:471 - Triggering progress: 4000/14700...
    2025-04-26 17:42:09,039 - INFO - 159189309:471 - Triggering progress: 4200/14700...
    2025-04-26 17:42:09,261 - INFO - 159189309:471 - Triggering progress: 4400/14700...
    2025-04-26 17:42:09,499 - INFO - 159189309:471 - Triggering progress: 4600/14700...
    2025-04-26 17:42:09,712 - INFO - 159189309:471 - Triggering progress: 4800/14700...
    2025-04-26 17:42:09,943 - INFO - 159189309:471 - Triggering progress: 5000/14700...
    2025-04-26 17:42:10,163 - INFO - 159189309:471 - Triggering progress: 5200/14700...
    2025-04-26 17:42:10,414 - INFO - 159189309:471 - Triggering progress: 5400/14700...
    2025-04-26 17:42:10,624 - INFO - 159189309:471 - Triggering progress: 5600/14700...
    2025-04-26 17:42:10,842 - INFO - 159189309:471 - Triggering progress: 5800/14700...
    2025-04-26 17:42:11,071 - INFO - 159189309:471 - Triggering progress: 6000/14700...
    2025-04-26 17:42:11,324 - INFO - 159189309:471 - Triggering progress: 6200/14700...
    2025-04-26 17:42:11,549 - INFO - 159189309:471 - Triggering progress: 6400/14700...
    2025-04-26 17:42:11,766 - INFO - 159189309:471 - Triggering progress: 6600/14700...
    2025-04-26 17:42:11,982 - INFO - 159189309:471 - Triggering progress: 6800/14700...
    2025-04-26 17:42:12,220 - INFO - 159189309:471 - Triggering progress: 7000/14700...
    2025-04-26 17:42:12,432 - INFO - 159189309:471 - Triggering progress: 7200/14700...
    2025-04-26 17:42:12,658 - INFO - 159189309:471 - Triggering progress: 7400/14700...
    2025-04-26 17:42:12,865 - INFO - 159189309:471 - Triggering progress: 7600/14700...
    2025-04-26 17:42:13,098 - INFO - 159189309:471 - Triggering progress: 7800/14700...
    2025-04-26 17:42:13,326 - INFO - 159189309:471 - Triggering progress: 8000/14700...
    2025-04-26 17:42:13,526 - INFO - 159189309:471 - Triggering progress: 8200/14700...
    2025-04-26 17:42:13,734 - INFO - 159189309:471 - Triggering progress: 8400/14700...
    2025-04-26 17:42:13,934 - INFO - 159189309:471 - Triggering progress: 8600/14700...
    2025-04-26 17:42:14,146 - INFO - 159189309:471 - Triggering progress: 8800/14700...
    2025-04-26 17:42:14,355 - INFO - 159189309:471 - Triggering progress: 9000/14700...
    2025-04-26 17:42:14,581 - INFO - 159189309:471 - Triggering progress: 9200/14700...
    2025-04-26 17:42:14,789 - INFO - 159189309:471 - Triggering progress: 9400/14700...
    2025-04-26 17:42:15,044 - INFO - 159189309:471 - Triggering progress: 9600/14700...
    2025-04-26 17:42:15,268 - INFO - 159189309:471 - Triggering progress: 9800/14700...
    2025-04-26 17:42:15,495 - INFO - 159189309:471 - Triggering progress: 10000/14700...
    2025-04-26 17:42:15,714 - INFO - 159189309:471 - Triggering progress: 10200/14700...
    2025-04-26 17:42:15,973 - INFO - 159189309:471 - Triggering progress: 10400/14700...
    2025-04-26 17:42:16,183 - INFO - 159189309:471 - Triggering progress: 10600/14700...
    2025-04-26 17:42:16,380 - INFO - 159189309:471 - Triggering progress: 10800/14700...
    2025-04-26 17:42:16,614 - INFO - 159189309:471 - Triggering progress: 11000/14700...
    2025-04-26 17:42:16,827 - INFO - 159189309:471 - Triggering progress: 11200/14700...
    2025-04-26 17:42:17,071 - INFO - 159189309:471 - Triggering progress: 11400/14700...
    2025-04-26 17:42:17,272 - INFO - 159189309:471 - Triggering progress: 11600/14700...
    2025-04-26 17:42:17,486 - INFO - 159189309:471 - Triggering progress: 11800/14700...
    2025-04-26 17:42:17,691 - INFO - 159189309:471 - Triggering progress: 12000/14700...
    2025-04-26 17:42:17,889 - INFO - 159189309:471 - Triggering progress: 12200/14700...
    2025-04-26 17:42:18,098 - INFO - 159189309:471 - Triggering progress: 12400/14700...
    2025-04-26 17:42:18,304 - INFO - 159189309:471 - Triggering progress: 12600/14700...
    2025-04-26 17:42:18,530 - INFO - 159189309:471 - Triggering progress: 12800/14700...
    2025-04-26 17:42:18,729 - INFO - 159189309:471 - Triggering progress: 13000/14700...
    2025-04-26 17:42:18,930 - INFO - 159189309:471 - Triggering progress: 13200/14700...
    2025-04-26 17:42:19,128 - INFO - 159189309:471 - Triggering progress: 13400/14700...
    2025-04-26 17:42:19,370 - INFO - 159189309:471 - Triggering progress: 13600/14700...
    2025-04-26 17:42:19,573 - INFO - 159189309:471 - Triggering progress: 13800/14700...
    2025-04-26 17:42:19,811 - INFO - 159189309:471 - Triggering progress: 14000/14700...
    2025-04-26 17:42:20,022 - INFO - 159189309:471 - Triggering progress: 14200/14700...
    2025-04-26 17:42:20,234 - INFO - 159189309:471 - Triggering progress: 14400/14700...
    2025-04-26 17:42:20,432 - INFO - 159189309:471 - Triggering progress: 14600/14700...
    2025-04-26 17:42:20,526 - INFO - 159189309:471 - Triggering progress: 14700/14700...
    2025-04-26 17:42:20,529 - INFO - 159189309:473 - Parallel triggering attempts complete. Success (Published): 14700, Failed/Skipped: 0
    2025-04-26 17:42:20,530 - INFO - 159189309:481 - Discovery and Trigger process finished in 28.51 seconds.
    2025-04-26 17:42:20,541 - INFO - 159189309:490 - ETL Discovery & Intake (Cell 1 - Batch Inserts/Updates v6 FINAL) finished.



```python
#cell2 pic exists for triggering Cell 3.
#       Includes shared naming functions (should be moved to utils) & validation.

import logging
import os
import json
import base64
import time
import re
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

from google.cloud import bigquery, storage, pubsub_v1
from google.api_core.exceptions import NotFound, Conflict, ServiceUnavailable, GoogleAPIError, BadRequest
from google.cloud import exceptions as google_exceptions
# Import Retry and the predicate helper function for google-cloud-core retry
from google.api_core.retry import Retry, if_exception_type
# Import tenacity components needed for the factory function
from tenacity import retry as tenacity_retry, stop_after_attempt, wait_exponential, retry_if_exception_type as tenacity_if_exception_type

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s'
)
logger = logging.getLogger(__name__)
# (Reduce verbosity logs - same as before)
logging.getLogger('google.api_core').setLevel(logging.WARNING)
logging.getLogger('google.auth').setLevel(logging.WARNING)
logging.getLogger('google.cloud').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)


# --- Configuration ---
PROJECT_ID = os.environ.get('PROJECT_ID', "spry-bus-425315-p6")
DATASET_ID = os.environ.get('DATASET_ID', "mycocoons")
LOCATION = os.environ.get('LOCATION', "US") # BQ Location
MANIFEST_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.pipeline_manifest"
CLEANING_PUBSUB_TOPIC_ID = os.environ.get('PUBSUB_CLEANING_TOPIC', 'etl-cleaning-requests')

# --- Retry Configuration ---
RETRYABLE_EXCEPTIONS_TENACITY = ( GoogleAPIError, ConnectionError, TimeoutError, ServiceUnavailable, BadRequest )
def retry_with_backoff_tenacity(retries=3, backoff_in_seconds=1, max_wait=10, retry_exceptions=RETRYABLE_EXCEPTIONS_TENACITY):
    if not isinstance(retry_exceptions, tuple): retry_exceptions = (retry_exceptions,)
    return tenacity_retry(
        retry=tenacity_if_exception_type(retry_exceptions), stop=stop_after_attempt(retries),
        wait=wait_exponential(multiplier=backoff_in_seconds, min=1, max=max_wait),
        before_sleep=lambda rs: logger.warning(f"Retrying {rs.fn.__name__} due to {rs.outcome.exception()} (attempt {rs.attempt_number})"),
        reraise=True
    )

bq_load_retry = Retry( initial=1.0, maximum=60.0, multiplier=2.0, deadline=600.0, predicate=if_exception_type(ServiceUnavailable, GoogleAPIError) )


# --- GCP Clients ---
bq_client: Optional[bigquery.Client] = None
storage_client: Optional[storage.Client] = None
pubsub_publisher: Optional[pubsub_v1.PublisherClient] = None
CLEANING_TOPIC_PATH: Optional[str] = None

# --- Client Initialization ---
def initialize_clients(max_retries=3):
    """Initializes clients needed for this service."""
    global bq_client, storage_client, pubsub_publisher, CLEANING_TOPIC_PATH
    if bq_client and storage_client and (pubsub_publisher or not CLEANING_PUBSUB_TOPIC_ID):
        logger.debug("Clients already initialized.")
        return

    logger.info("Attempting to initialize GCP clients (BigQuery, Storage, Pub/Sub)...")
    retry_count = 0
    while retry_count < max_retries:
        try:
            if not bq_client:
                bq_client = bigquery.Client(project=PROJECT_ID)
                bq_client.list_datasets(max_results=1) # Verify connection
                logger.info("BigQuery client initialized.")
            if not storage_client:
                storage_client = storage.Client(project=PROJECT_ID)
                logger.info("Storage client initialized.")
            if CLEANING_PUBSUB_TOPIC_ID and not pubsub_publisher:
                try:
                    pubsub_publisher = pubsub_v1.PublisherClient()
                    CLEANING_TOPIC_PATH = pubsub_publisher.topic_path(PROJECT_ID, CLEANING_PUBSUB_TOPIC_ID)
                    pubsub_publisher.get_topic(request={"topic": CLEANING_TOPIC_PATH}) # Check existence
                    logger.info(f"Pub/Sub publisher initialized for cleaning topic: {CLEANING_TOPIC_PATH}")
                except NotFound:
                    logger.error(f"Pub/Sub topic '{CLEANING_PUBSUB_TOPIC_ID}' not found. Triggering disabled.")
                    pubsub_publisher = None; CLEANING_TOPIC_PATH = None
                except Exception as pubsub_err:
                     logger.error(f"Failed to initialize Pub/Sub publisher: {pubsub_err}", exc_info=True)
                     pubsub_publisher = None; CLEANING_TOPIC_PATH = None

            if bq_client and storage_client: # Check essential clients
                 logger.info("Successfully initialized required GCP clients.")
                 return

        except Exception as e:
            retry_count += 1; wait_time = 2 ** retry_count
            logger.warning(f"Client init error: {e}. Retry {retry_count}/{max_retries} in {wait_time}s", exc_info=False)
            bq_client = storage_client = pubsub_publisher = None; CLEANING_TOPIC_PATH = None # Reset on error
            if retry_count >= max_retries:
                logger.error("Client init failed after retries."); raise RuntimeError("Client initialization failed")
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

def get_staging_table_id(blob_name: str, project_id: str = PROJECT_ID, dataset_id: str = DATASET_ID) -> Optional[str]:
    """Determines the target staging table name (stg_<source>_<identifier>_<date>)"""
    if not blob_name: return None
    parts = blob_name.split('/')
    source_index = 1 if parts[0].lower() == 'mycocoons' and len(parts) > 1 else 0
    source = parts[source_index] if len(parts) > source_index else "unknown_source"
    filename = os.path.splitext(os.path.basename(blob_name))[0]

    date_match = re.search(r'(\d{8})$', filename) # Match YYYYMMDD at the end
    date_suffix = f"_{date_match.group(1)}" if date_match else ""
    identifier = re.sub(r'(_\d{8})$', '', filename) # Remove date suffix for identifier

    sanitized_source = _sanitize_bq_name(source)
    sanitized_identifier = _sanitize_bq_name(identifier)

    table_name = f"stg_{sanitized_source}_{sanitized_identifier}{date_suffix}"

    # ** Validation **
    if not re.match(r"^[a-zA-Z0-9_]+$", table_name):
        logger.error(f"Generated table name contains invalid characters: '{table_name}' from blob '{blob_name}'")
        raise ValueError(f"Invalid table name generated: {table_name}")
    if len(table_name) > 1024: # BQ limit
        logger.warning(f"Generated table name exceeds 1024 chars, will be truncated: '{table_name}'")
        table_name = table_name[:1024]

    full_table_id = f"{project_id}.{dataset_id}.{table_name}"
    logger.info(f"Determined target table ID: {full_table_id} for blob {blob_name}")
    return full_table_id

from google.cloud import bigquery
import logging

logger = logging.getLogger("journey_builder")
logging.basicConfig(level=logging.INFO)

PROJECT_ID = "spry-bus-425315-p6"
DATASET_ID = "mycocoons"
LOCATION = "US"
bq_client = bigquery.Client(project=PROJECT_ID, location=LOCATION)

def list_clean_tables_for_date(dataset: str, date_suffix: str):
    """Returns a list of clean table names in BigQuery with the specified date suffix."""
    table_list = list(bq_client.list_tables(dataset))
    matching_tables = [
        t.table_id for t in table_list
        if t.table_id.startswith("clean_mycocoons_") and t.table_id.endswith(date_suffix)
    ]
    logger.info(f"Found tables for {date_suffix}: {matching_tables}")
    return matching_tables

# =============================================================================


# --- Manifest Functions ---
# (Keep check_blob_in_manifest and add_or_update_blob_in_manifest as before)
@retry_with_backoff_tenacity()
def check_blob_in_manifest(blob_name: str) -> Optional[Dict[str, Any]]:
    """Checks the latest manifest entry for a blob."""
    if not bq_client: raise RuntimeError("BigQuery client not initialized.")
    query = f"SELECT processing_status, retry_count FROM `{MANIFEST_TABLE_ID}` WHERE blob_name = @blob_name ORDER BY discovered_at DESC LIMIT 1"
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("blob_name", "STRING", blob_name)])
    try:
        results = list(bq_client.query(query, job_config=job_config).result(timeout=60))
        return dict(results[0].items()) if results else None
    except Exception as e: logger.error(f"Error checking manifest for blob {blob_name}: {e}", exc_info=True); raise

@retry_with_backoff_tenacity(retries=4, backoff_in_seconds=1.5, max_wait=15)
def add_or_update_blob_in_manifest(blob: storage.Blob, status: str, error_msg: Optional[str] = None, target_table: Optional[str] = None) -> None:
    """Adds or updates a blob entry in the manifest table using MERGE."""
    if not bq_client: raise RuntimeError("BigQuery client not initialized.")
    if not blob: logger.error("Invalid blob object passed to manifest update."); return
    now_ts_utc = datetime.utcnow()
    processed_ts = now_ts_utc if status in ["SUCCESS", "FAILED"] else None
    # Ensure target_table is full path if provided
    if target_table and '.' not in target_table:
        target_table = f"{PROJECT_ID}.{DATASET_ID}.{target_table.split('.')[-1]}" # Use only table name part if full path wasn't passed

    params = [bigquery.ScalarQueryParameter("blob_name", "STRING", blob.name), bigquery.ScalarQueryParameter("bucket_name", "STRING", blob.bucket.name), bigquery.ScalarQueryParameter("size_bytes", "INTEGER", blob.size), bigquery.ScalarQueryParameter("content_type", "STRING", blob.content_type), bigquery.ScalarQueryParameter("md5_hash", "STRING", blob.md5_hash), bigquery.ScalarQueryParameter("crc32c", "STRING", blob.crc32c), bigquery.ScalarQueryParameter("generation", "INTEGER", blob.generation), bigquery.ScalarQueryParameter("discovered_at", "TIMESTAMP", now_ts_utc), bigquery.ScalarQueryParameter("processed_at", "TIMESTAMP", processed_ts), bigquery.ScalarQueryParameter("processing_status", "STRING", status), bigquery.ScalarQueryParameter("target_table", "STRING", target_table), bigquery.ScalarQueryParameter("error_message", "STRING", error_msg)]
    merge_sql = f""" MERGE `{MANIFEST_TABLE_ID}` T USING (SELECT @blob_name AS blob_name) S ON T.blob_name = S.blob_name WHEN MATCHED THEN UPDATE SET processing_status=@processing_status, processed_at=COALESCE(@processed_at,T.processed_at), error_message=@error_message, target_table=COALESCE(@target_table,T.target_table), retry_count=T.retry_count+CASE WHEN T.processing_status!='FAILED' AND @processing_status='FAILED' THEN 1 WHEN T.processing_status='FAILED' AND @processing_status='PENDING' THEN 0 WHEN T.processing_status='FAILED' AND @processing_status='FAILED' THEN 1 ELSE 0 END, size_bytes=CASE WHEN T.generation!=@generation THEN @size_bytes ELSE T.size_bytes END, md5_hash=CASE WHEN T.generation!=@generation THEN @md5_hash ELSE T.md5_hash END, crc32c=CASE WHEN T.generation!=@generation THEN @crc32c ELSE T.crc32c END, generation=@generation, content_type=@content_type WHEN NOT MATCHED THEN INSERT (blob_name, bucket_name, size_bytes, content_type, md5_hash, crc32c, generation, discovered_at, processed_at, processing_status, target_table, error_message, retry_count) VALUES (@blob_name, @bucket_name, @size_bytes, @content_type, @md5_hash, @crc32c, @generation, @discovered_at, @processed_at, @processing_status, @target_table, @error_message, CASE WHEN @processing_status='FAILED' THEN 1 ELSE 0 END) """
    job_config=bigquery.QueryJobConfig(query_parameters=params)
    try:
        logger.info(f"Updating manifest: {blob.name} -> {status}.")
        query_job=bq_client.query(merge_sql, job_config=job_config)
        query_job.result(timeout=120)
        logger.info(f"Manifest update OK: {blob.name}.")
    except Exception as e:
        if "streaming buffer" in str(e).lower(): logger.warning(f"Manifest MERGE conflict for {blob.name}. Will retry. Error: {e}")
        else: logger.error(f"Manifest MERGE error for {blob.name}: {e}", exc_info=True)
        raise


# --- Core Ingestion Logic ---
def load_blob_to_bigquery(blob: storage.Blob, target_table_id: str) -> Tuple[bool, Optional[str]]:
    """Loads a single blob into a BigQuery table using load_table_from_uri."""
    # *** FIX applied: Separated initial checks ***
    if not bq_client:
        raise RuntimeError("BigQuery client not initialized.")
    if not blob:
        logger.error("Invalid blob object passed to load_blob_to_bigquery.")
        return False, None
    if not target_table_id:
        logger.error("Target table ID not provided to load_blob_to_bigquery.")
        return False, None

    uri = f"gs://{blob.bucket.name}/{blob.name}"
    try:
        table_ref = bigquery.TableReference.from_string(target_table_id)
    except ValueError as e:
        logger.error(f"Invalid target table ID format '{target_table_id}': {e}")
        return False, None # Return None for table_id as it was invalid

    source_format = None
    if blob.name.lower().endswith('.csv'): source_format=bigquery.SourceFormat.CSV
    elif blob.name.lower().endswith(('.json','.jsonl')): source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    elif blob.name.lower().endswith('.parquet'): source_format=bigquery.SourceFormat.PARQUET
    if not source_format:
        logger.error(f"Unsupported format: {blob.name}")
        return False, target_table_id

    job_config=bigquery.LoadJobConfig(source_format=source_format,autodetect=True,write_disposition="WRITE_TRUNCATE",create_disposition="CREATE_IF_NEEDED")
    if source_format == bigquery.SourceFormat.CSV:
        job_config.skip_leading_rows=1
        job_config.allow_quoted_newlines=True
        job_config.ignore_unknown_values=True

    logger.info(f"Starting load: {uri} -> {target_table_id}")
    load_job=None
    try:
        load_job=bq_client.load_table_from_uri(uri,table_ref,job_config=job_config,retry=bq_load_retry)
        logger.info(f"Load job: {load_job.job_id}. Waiting...")
        load_job.result(timeout=600) # 10 min timeout

        if load_job.errors:
            error_str="; ".join([f"{err.get('reason','?')}: {err.get('message','?')}" for err in load_job.errors])
            logger.error(f"Load job {load_job.job_id} failed: {error_str}")
            raise GoogleAPIError(f"Load job failed: {error_str}")
        else:
            final_job=bq_client.get_job(load_job.job_id, location=load_job.location) # Add location
            rows_loaded = final_job.output_rows if hasattr(final_job,'output_rows') else 'unknown'
            logger.info(f"Load job {load_job.job_id} OK. Rows: {rows_loaded} -> {target_table_id}")
            return True, target_table_id
    except Exception as e:
        job_id_str=f" (Job ID: {load_job.job_id})" if load_job else ""
        logger.error(f"Load failed for {uri}{job_id_str}: {e}",exc_info=True)
        raise e # Reraise the exception


# --- Triggering Next Step ---
@retry_with_backoff_tenacity(retries=2)
def trigger_cleaning_job(target_table_id: str, blob_name: str):
    """Publishes a message to trigger the cleaning job (Cell 3)."""
    if not pubsub_publisher or not CLEANING_TOPIC_PATH:
        logger.warning(f"Pub/Sub cleaning trigger disabled for {CLEANING_PUBSUB_TOPIC_ID}. Skipping.")
        return

    message_data = { "target_table_id": target_table_id, "source_blob_name": blob_name, "trigger_timestamp": datetime.utcnow().isoformat() }
    message_body = json.dumps(message_data).encode("utf-8")
    try:
        future = pubsub_publisher.publish(CLEANING_TOPIC_PATH, message_body)
        future.result(timeout=30)
        logger.info(f"Published cleaning request for {target_table_id} to {CLEANING_PUBSUB_TOPIC_ID}.")
    except Exception as e:
        logger.error(f"Failed cleaning publish for {target_table_id}: {e}", exc_info=True)
        raise

# --- Main Handler Function (for Cloud Function/Run) ---
def handle_gcs_event(event: Dict[str, Any], context: Any):
    """Cloud Function/Run entry point triggered by Pub/Sub message from Cell 1."""
    start_time = time.time()
    try: initialize_clients()
    except Exception as init_err: logger.critical(f"Client init failed: {init_err}", exc_info=True); return

    blob_name=None; blob=None; target_table_id=None; status=None; error_message=None; load_exception=None; processed_or_failed=False

    try:
        if 'data' not in event: raise ValueError("No 'data' field")
        message_data=json.loads(base64.b64decode(event['data']).decode('utf-8'))
        bucket_name=message_data.get('bucket_name'); blob_name=message_data.get('blob_name')
        if not bucket_name or not blob_name: raise ValueError("Missing bucket/blob name")
        logger.info(f"Received trigger for gs://{bucket_name}/{blob_name}")

        blob=storage_client.bucket(bucket_name).get_blob(blob_name)
        if not blob: raise FileNotFoundError(f"Blob not found: gs://{bucket_name}/{blob_name}")
        if blob.size==0: logger.info(f"Skipping zero-byte blob: {blob_name}"); status="SKIPPED_EMPTY"; return

        manifest_entry=check_blob_in_manifest(blob_name)
        if manifest_entry:
            current_status=manifest_entry.get('processing_status')
            if current_status=="SUCCESS": logger.info(f"Blob {blob_name} already SUCCESS. Skipping."); status="SKIPPED_ALREADY_SUCCESS"; return
            elif current_status=="PROCESSING": logger.warning(f"Blob {blob_name} already PROCESSING. Skipping."); status="SKIPPED_ALREADY_PROCESSING"; return

        processed_or_failed=True; status="FAILED"; error_message="Processing did not complete."
        add_or_update_blob_in_manifest(blob, status="PROCESSING") # Update status FIRST

        target_table_id=get_staging_table_id(blob_name) # Use helper
        if not target_table_id: raise ValueError(f"Cannot determine target table for {blob_name}")

        load_success, actual_table_id = load_blob_to_bigquery(blob, target_table_id)
        target_table_id = actual_table_id if actual_table_id else target_table_id # Use returned ID

        if load_success: status="SUCCESS"; error_message=None; logger.info(f"Successfully loaded {blob_name} -> {target_table_id}")
        else: error_message=f"load_blob_to_bigquery failed for {blob_name}"; raise RuntimeError(error_message) # Should be caught by general Exception

    except FileNotFoundError as e: status="FAILED"; error_message=str(e); logger.error(error_message); processed_or_failed=True
    except ValueError as e: status="FAILED"; error_message=f"Validation error for {blob_name}: {str(e)}"; logger.error(error_message); processed_or_failed=True
    except Exception as e: status="FAILED"; load_exception=e; error_message=f"Error processing {blob_name}: {str(e)}"; logger.error(error_message, exc_info=True); processed_or_failed=True

    finally:
        final_status_to_log = "UNKNOWN"
        if processed_or_failed and blob:
            final_error_msg=str(load_exception) if load_exception else error_message
            try: add_or_update_blob_in_manifest(blob, status=status, error_msg=final_error_msg, target_table=target_table_id if status=="SUCCESS" else None); final_status_to_log=status
            except Exception as manifest_err: logger.error(f"CRITICAL: Failed manifest update to {status} for {blob_name}: {manifest_err}", exc_info=True); final_status_to_log=f"{status} (MANIFEST_UPDATE_FAILED)"
        elif status and "SKIPPED" in status: logger.info(f"Processing skipped for {blob_name}. Reason: {status}"); final_status_to_log=status
        elif blob_name: logger.error(f"Cannot update manifest for {blob_name} (no blob object/no processing attempt)."); final_status_to_log="FAILED (PRE_PROCESS_ERROR)"
        else: logger.error("Cannot update manifest status; blob name unknown."); final_status_to_log="FAILED (UNKNOWN_BLOB)"

        if status=="SUCCESS" and target_table_id:
            try: trigger_cleaning_job(target_table_id, blob_name)
            except Exception as trigger_err: logger.error(f"Loaded {blob_name}, but failed cleaning trigger: {trigger_err}", exc_info=True)

        end_time=time.time(); logger.info(f"Finished trigger {blob_name or 'unknown blob'} in {end_time-start_time:.2f}s. Status: {final_status_to_log}")
```


```python
# Cell 3: Cleaning Service (DEPLOYMENT READY v6 – Integrated Cleaning Logic)
# NOTE: Deploy as Cloud Function/Run service triggered by 'etl-cleaning-requests' topic.
#       Ensure 'etl-catalog-requests' and 'etl-journey-requests' topics exist for Cells 4 and 3‑5.
#       Combines the naming + retry fixes from v5 with data‑standardisation logic
#       extracted from the MoA Layer‑1 agent in code_cell6.*.
# ──────────────────────────────────────────────────────────────────────────────

import pandas as pd
from google.cloud import bigquery, pubsub_v1
import logging, os, time, json, base64, random, re, inspect
from datetime import datetime
from typing import Dict, Any, Optional, Callable, List
from google.api_core.exceptions import NotFound, GoogleAPIError, ServiceUnavailable, BadRequest
from tenacity import retry as tenacity_retry, stop_after_attempt, wait_exponential, retry_if_exception_type as tenacity_if_exception_type

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s')
logger = logging.getLogger(__name__)
for noisy in ['google.api_core', 'google.auth', 'google.cloud', 'urllib3']:
    logging.getLogger(noisy).setLevel(logging.WARNING)

# ── Environment / Config ─────────────────────────────────────────────────────
PROJECT_ID = os.getenv('PROJECT_ID', 'spry-bus-425315-p6')
DATASET_ID = os.getenv('DATASET_ID', 'mycocoons')
LOCATION   = os.getenv('LOCATION',   'US')  # BigQuery location
CATALOG_PUBSUB_TOPIC_ID = os.getenv('PUBSUB_CATALOG_TOPIC',  'etl-catalog-requests')
JOURNEY_PUBSUB_TOPIC_ID = os.getenv('PUBSUB_JOURNEY_TOPIC',  'etl-journey-requests')

# ── Retry wrapper ────────────────────────────────────────────────────────────
RETRYABLE_EXC = (GoogleAPIError, ConnectionError, TimeoutError, ServiceUnavailable, BadRequest)

def retry_with_backoff(retries: int = 3, backoff: float = 1, max_wait: int = 10):
    return tenacity_retry(
        retry  = tenacity_if_exception_type(RETRYABLE_EXC),
        stop   = stop_after_attempt(retries),
        wait   = wait_exponential(multiplier=backoff, min=1, max=max_wait),
        before_sleep=lambda rs: logger.warning(f"Retrying {rs.fn.__name__} ({rs.attempt_number})"),
        reraise=True,
    )

# ── GCP clients (lazy) ───────────────────────────────────────────────────────
bq_client: Optional[bigquery.Client]     = None
pub_client: Optional[pubsub_v1.PublisherClient] = None
CATALOG_PATH: Optional[str] = None
JOURNEY_PATH: Optional[str] = None

@retry_with_backoff()
def init_clients():
    global bq_client, pub_client, CATALOG_PATH, JOURNEY_PATH
    if bq_client and (pub_client or not (CATALOG_PUBSUB_TOPIC_ID or JOURNEY_PUBSUB_TOPIC_ID)):
        return

    if not bq_client:
        bq_client = bigquery.Client(project=PROJECT_ID)
        bq_client.list_datasets(max_results=1)
        logger.info('BigQuery client ready')

    if (CATALOG_PUBSUB_TOPIC_ID or JOURNEY_PUBSUB_TOPIC_ID) and not pub_client:
        pub_client = pubsub_v1.PublisherClient()
        if CATALOG_PUBSUB_TOPIC_ID:
            try:
                CATALOG_PATH = pub_client.topic_path(PROJECT_ID, CATALOG_PUBSUB_TOPIC_ID)
                pub_client.get_topic(request={"topic": CATALOG_PATH})
            except NotFound:
                logger.error(f"Catalog topic {CATALOG_PUBSUB_TOPIC_ID} missing – catalog trigger disabled")
                CATALOG_PATH = None
        if JOURNEY_PUBSUB_TOPIC_ID:
            try:
                JOURNEY_PATH = pub_client.topic_path(PROJECT_ID, JOURNEY_PUBSUB_TOPIC_ID)
                pub_client.get_topic(request={"topic": JOURNEY_PATH})
            except NotFound:
                logger.error(f"Journey topic {JOURNEY_PUBSUB_TOPIC_ID} missing – journey trigger disabled")
                JOURNEY_PATH = None
        logger.info('Pub/Sub publisher ready')

# ── Naming helpers ───────────────────────────────────────────────────────────

def _sanitize(name: str) -> str:
    out = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    return re.sub(r'_+', '_', out).strip('_').lower() or 'unknown'


def get_cleaned_table_id(stg_id: str) -> Optional[str]:
    parts = stg_id.split('.')
    if len(parts) != 3 or not parts[2].startswith('stg_'):
        logger.error(f'Invalid staging table id: {stg_id}')
        return None
    proj, dset, name = parts
    # New pattern clean_mycocoons_<Source>_<identifier>_<date>
    core = name[4:]  # drop stg_
    segs = core.split('_')
    if len(segs) < 2:
        logger.error(f'Cannot derive cleaned name from {name}')
        return None
    source_cap = segs[0].capitalize()
    date_sfx = segs[-1] if re.fullmatch(r'\d{8}', segs[-1]) else ''
    ident = '_'.join(segs[1:-1] if date_sfx else segs[1:]).upper()
    cleaned = f"clean_mycocoons_{source_cap}_{ident}{'_' + date_sfx if date_sfx else ''}"
    return f"{proj}.{dset}.{cleaned}"


def extract_source(table_name_or_id: str) -> Optional[str]:
    tbl = table_name_or_id.split('.')[-1]
    m = re.match(r'(?:stg|clean)_([a-z0-9]+?)_', tbl, re.I)
    if m:
        return m.group(1).lower()
    m2 = re.match(r'clean_mycocoons_([a-zA-Z0-9]+)_', tbl)
    return m2.group(1).lower() if m2 else None

# ── Generic standardisation (from MoA Layer‑1) ───────────────────────────────

def _standardize_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Generic schema harmoniser adapted from standardize_schema() in code_cell6.3.
    Ensures presence / types of core columns across sources."""
    mapping = {
        'customer_id': 'customer_id',
        'user_pseudo_id': 'customer_id',
        'ga_clientid': 'customer_id',
        'ga_userid': 'customer_id',
        'ad_id': 'customer_id',  # proxy where nothing better

        'event_timestamp': 'event_timestamp',
        'event_datetime':  'event_timestamp',
        'timestamp':       'event_timestamp',
        'created_at':      'event_timestamp',
        'date':            'event_timestamp',
        'session_date':    'event_timestamp',

        'event_name':  'event_type',
        'event':       'event_type',
        'event_type':  'event_type',

        'value':        'event_value',
        'revenue':      'event_value',
        'transaction_revenue': 'event_value',
    }
    df = df.rename(columns={c: mapping[c] for c in df.columns if c in mapping})

    required = ['customer_id', 'event_timestamp', 'event_type', 'event_value', 'source']
    for col in required:
        if col not in df.columns:
            df[col] = pd.NA

    df['event_timestamp'] = pd.to_datetime(df['event_timestamp'], errors='coerce', utc=True)
    df['event_value'] = pd.to_numeric(df['event_value'], errors='coerce')
    return df[required]

# ── Source‑specific cleaning functions ───────────────────────────────────────

def clean_facebook_ads(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    for col in ['impressions', 'clicks', 'reach', 'frequency', 'spend']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    df['source'] = 'Facebook'
    return _standardize_schema(df)


def clean_google_ads(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    for col in ['impressions', 'clicks', 'cost', 'conversions', 'ctr', 'cpc']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    df['source'] = 'GoogleAds'
    return _standardize_schema(df)


def clean_ga4(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df['source'] = 'GA4Custom'
    return _standardize_schema(df)


def clean_ga3(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df['source'] = 'GAAnalytics'
    return _standardize_schema(df)


def clean_shopify(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    # monetary columns
    for col in ['total_price', 'subtotal_price', 'total_discounts', 'total_tax']:
        if col in df.columns and df[col].dtype == object:
            df[col] = (df[col].str.replace('[\$,]', '', regex=True).astype(float))
    df['source'] = 'Shopify'
    return _standardize_schema(df)


def clean_generic(df: pd.DataFrame, table_id: str) -> pd.DataFrame:
    logger.warning(f'Generic cleaner engaged for {table_id}')
    df['source'] = extract_source(table_id) or 'unknown'
    return _standardize_schema(df)

# Map sources to cleaners
CLEANER_MAP: Dict[str, Callable[[pd.DataFrame], pd.DataFrame]] = {
    'facebook':   clean_facebook_ads,
    'google':     clean_google_ads,
    'ga4custom':  clean_ga4,
    'gaanalytics': clean_ga3,
    'shopify':    clean_shopify,
}

def pick_cleaner(stg_table_id: str) -> Callable[[pd.DataFrame], pd.DataFrame]:
    src = extract_source(stg_table_id)
    if src in CLEANER_MAP:
        return CLEANER_MAP[src]
    return lambda df: clean_generic(df, stg_table_id)

# ── Schema helpers + load (unchanged from v5 except cosmetic) ────────────────
@retry_with_backoff()
def ensure_schema(df: pd.DataFrame, ref: bigquery.TableReference):
    tbl = bq_client.get_table(ref) if bq_client else None
    if not tbl:
        return  # created automatically on load
    add = []
    current = {f.name: f for f in tbl.schema}
    for col in df.columns:
        if col in current:
            continue
        dtype = df[col].dtype
        ftype = 'STRING'
        if pd.api.types.is_integer_dtype(dtype): ftype = 'INT64'
        elif pd.api.types.is_float_dtype(dtype): ftype = 'FLOAT64'
        elif pd.api.types.is_datetime64_any_dtype(dtype): ftype = 'TIMESTAMP'
        elif pd.api.types.is_bool_dtype(dtype): ftype = 'BOOL'
        add.append(bigquery.SchemaField(col, ftype))
    if add:
        tbl.schema += add
        bq_client.update_table(tbl, ['schema'])

@retry_with_backoff()
def load_df(df: pd.DataFrame, ref: bigquery.TableReference):
    job_cfg = bigquery.LoadJobConfig(
        schema=bq_client.schema_from_dataframe(df),
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED')
    job = bq_client.load_table_from_dataframe(df, ref, job_config=job_cfg)
    job.result(timeout=600)
    logger.info(f"Loaded {job.output_rows} rows → {ref.table_id}")

# ── Pub/Sub triggers ─────────────────────────────────────────────────────────
@retry_with_backoff(retries=2)
def trigger_catalog(cleaned_id: str, stg_id: str):
    if not (pub_client and CATALOG_PATH):
        return
    msg = json.dumps({'cleaned_table_id': cleaned_id, 'source_stg_table': stg_id,
                      'trigger_timestamp': datetime.utcnow().isoformat()}).encode()
    pub_client.publish(CATALOG_PATH, msg).result(30)

@retry_with_backoff(retries=2)
def trigger_journey(date_suffix: str):
    if not (pub_client and JOURNEY_PATH):
        return
    msg = json.dumps({'date_suffix': date_suffix, 'trigger_timestamp': datetime.utcnow().isoformat()}).encode()
    pub_client.publish(JOURNEY_PATH, msg).result(30)

# ── Main handler (Cloud Function entry‑point) ────────────────────────────────

def handle_cleaning_request(event: Dict[str, Any], context: Any):  # noqa: C901 – long but manageable
    t0 = time.time()
    stg_id = cleaned_id = date_sfx = None
    status, err = 'FAILED', 'Initial failure'

    try:
        init_clients()
        data = json.loads(base64.b64decode(event['data']).decode())
        stg_id = data.get('target_table_id')
        if not stg_id:
            raise ValueError('target_table_id missing')
        date_m = re.search(r'_(\d{8})$', stg_id)
        date_sfx = date_m.group(1) if date_m else None
        cleaned_id = get_cleaned_table_id(stg_id)
        if not cleaned_id:
            raise ValueError('Could not derive cleaned table id')
        ref = bigquery.TableReference.from_string(cleaned_id)

        df = bq_client.query(f"SELECT * FROM `{stg_id}`",
                             job_config=bigquery.QueryJobConfig(use_query_cache=False)).to_dataframe()
        if df.empty:
            status, err = 'SUCCESS', 'Source empty – skipped'
            return

        cleaned_df = pick_cleaner(stg_id)(df.copy())
        if cleaned_df.empty:
            status, err = 'SUCCESS', 'Empty post‑clean – skipped'
            return

        ensure_schema(cleaned_df, ref)
        load_df(cleaned_df, ref)
        status, err = 'SUCCESS', None
    except Exception as e:
        err = str(e)
        logger.error(err, exc_info=True)
    finally:
        if status == 'SUCCESS' and cleaned_id and err is None:
            if date_sfx:
                trigger_journey(date_sfx)
            trigger_catalog(cleaned_id, stg_id)
        logger.info(f"Cleaning {stg_id or '??'} → {cleaned_id or '??'} completed in {time.time()-t0:.2f}s – {status}")

```


```python
# cell_3_5_journey_builder.py
import logging
import os
import time
import textwrap
from datetime import datetime, timedelta, date
from typing import List, Dict, Any, Optional, Tuple

from google.cloud import bigquery
from google.api_core.exceptions import NotFound, GoogleAPIError, ServiceUnavailable
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s'
)
logger = logging.getLogger(__name__)
logging.getLogger('google.api_core').setLevel(logging.WARNING)
logging.getLogger('google.auth').setLevel(logging.WARNING)
logging.getLogger('google.cloud').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

# --- Configuration ---
PROJECT_ID = os.environ.get('PROJECT_ID', "spry-bus-425315-p6")
DATASET_ID = os.environ.get('DATASET_ID', "mycocoons")
LOCATION = os.environ.get('LOCATION', "US") # BQ Location

# Target table for unified journey events
JOURNEY_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.cust_journey_events"

# Processing Date Range (Default to yesterday)
DEFAULT_DAYS_TO_PROCESS = 1
processing_date_str = os.environ.get(
    'PROCESSING_DATE', (date.today() - timedelta(days=DEFAULT_DAYS_TO_PROCESS)).strftime('%Y%m%d')
)
try:
    TARGET_DATE = datetime.strptime(processing_date_str, '%Y%m%d').date()
    # Assuming table suffixes match YYYYMMDD
    TARGET_DATE_SUFFIX = TARGET_DATE.strftime('%Y%m%d')
    logger.info(f"Targeting processing for date suffix: {TARGET_DATE_SUFFIX}")
except ValueError:
    logger.error(f"Invalid PROCESSING_DATE format: {processing_date_str}. Use YYYYMMDD. Exiting.")
    exit(1)

# --- Retry Configuration ---
RETRYABLE_EXCEPTIONS = (GoogleAPIError, ConnectionError, TimeoutError, ServiceUnavailable)
retry_with_backoff = retry(
    retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1.5, min=2, max=30),
    before_sleep=lambda rs: logger.warning(f"Retrying {rs.fn.__name__} due to {rs.outcome.exception()} (attempt {rs.attempt_number})"),
    reraise=True
)

# --- GCP Clients ---
bq_client: Optional[bigquery.Client] = None

def initialize_clients():
    """Initializes BigQuery client."""
    global bq_client
    if bq_client: return
    logger.info("Attempting to initialize BigQuery client...")
    try:
        bq_client = bigquery.Client(project=PROJECT_ID)
        bq_client.list_datasets(max_results=1)
        logger.info("Successfully initialized BigQuery client.")
    except Exception as e:
        logger.error(f"Failed to initialize BigQuery client: {e}", exc_info=True)
        raise RuntimeError("Client initialization failed")

def list_clean_tables_for_date(dataset: str, date_suffix: str):
    """Returns a list of clean table names in BigQuery with the specified date suffix."""
    if not bq_client:
        raise RuntimeError("BigQuery client not initialized. Call initialize_clients() first.")
    
    table_list = list(bq_client.list_tables(dataset))
    # FIX: Look for tables with the pattern that Cell 3 actually produces
    matching_tables = [
        t.table_id for t in table_list
        if t.table_id.startswith("clean_") and t.table_id.endswith(date_suffix)
    ]
    logger.info(f"Found tables for {date_suffix}: {matching_tables}")
    return matching_tables

# --- SQL Generation ---
def generate_journey_sql_dyn(project: str, dataset: str, date_suffix: str, output_table: str):
    """Generates SQL for journey table based on discovered clean tables."""
    if not bq_client:
        raise RuntimeError("BigQuery client not initialized. Call initialize_clients() first.")
    
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
        # Extend with other sources as needed

    # Create the full SQL query with UNION ALL between source blocks
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

@retry_with_backoff
def run_bq_job(sql: str):
    """Runs a BigQuery SQL job."""
    if not bq_client:
        raise RuntimeError("BigQuery client not initialized.")
    
    logger.info("Starting BigQuery job to build customer journey table...")
    logger.debug(f"Executing SQL:\n{sql[:1000]}...") # Log truncated SQL

    job_config = bigquery.QueryJobConfig(
        priority=bigquery.QueryPriority.BATCH # Use batch for potentially long-running job
    )

    query_job = bq_client.query(sql, job_config=job_config, location=LOCATION)
    logger.info(f"BigQuery job started. Job ID: {query_job.job_id}")

    # Wait for job completion
    query_job.result(timeout=3600) # Wait up to 1 hour

    if query_job.errors:
        logger.error(f"BigQuery job {query_job.job_id} failed: {query_job.errors}")
        raise GoogleAPIError(f"BigQuery job failed: {query_job.errors}")
    else:
        logger.info(f"BigQuery job {query_job.job_id} completed successfully.")
        # Log details about the created/replaced table
        dest_table_id = f"{PROJECT_ID}.{DATASET_ID}.cust_journey_events_{TARGET_DATE_SUFFIX}"
        dest_table = bq_client.get_table(dest_table_id)
        logger.info(f"Table `{dest_table_id}` updated. Rows: {dest_table.num_rows}, Size: {dest_table.num_bytes / (1024**2):.2f} MB")

def main():
    """Main function to build the customer journey table."""
    logger.info(f"Starting Customer Journey Builder for date suffix: {TARGET_DATE_SUFFIX}...")
    start_time = time.time()

    try:
        # Initialize clients first - fixes the error
        initialize_clients()

        # Generate the dynamic SQL
        sql = generate_journey_sql_dyn(
            project=PROJECT_ID,
            dataset=DATASET_ID,
            date_suffix=TARGET_DATE_SUFFIX,
            output_table="cust_journey_events"
        )

        # Run the BigQuery job
        run_bq_job(sql)

        end_time = time.time()
        logger.info(f"Customer Journey Builder finished successfully in {end_time - start_time:.2f} seconds.")

        # Optional: Trigger the next step in the pipeline (e.g., Cell 5)
        # trigger_kg_build_job(JOURNEY_TABLE_ID, TARGET_DATE_SUFFIX)

    except Exception as e:
        logger.critical(f"Customer Journey Builder failed: {e}", exc_info=True)
        # Add alerting or monitoring hooks here

if __name__ == "__main__":
    main()
```

    2025-04-27 14:55:29,755 - INFO - 2638794007:41 - Targeting processing for date suffix: 20250426
    2025-04-27 14:55:29,758 - INFO - 2638794007:221 - Starting Customer Journey Builder for date suffix: 20250426...
    2025-04-27 14:55:29,759 - INFO - 2638794007:63 - Attempting to initialize BigQuery client...
    2025-04-27 14:55:29,766 - INFO - 2638794007:67 - Successfully initialized BigQuery client.
    2025-04-27 14:56:29,256 - INFO - 2638794007:83 - Found tables for 20250426: []
    2025-04-27 14:56:29,271 - CRITICAL - 2638794007:246 - Customer Journey Builder failed: No clean tables found for date 20250426! Cannot build journey table.
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_5004/2638794007.py", line 229, in main
        sql = generate_journey_sql_dyn(
      File "/var/tmp/ipykernel_5004/2638794007.py", line 94, in generate_journey_sql_dyn
        raise RuntimeError(f"No clean tables found for date {date_suffix}! Cannot build journey table.")
    RuntimeError: No clean tables found for date 20250426! Cannot build journey table.



```python
# Cell 4.1: Data Dictionary Population (Fixed v2 - Corrected Type Hint)
# NOTE: Remediation Report suggests merging this with 4.2. This version is standalone but includes fixes.
#       Assumes it runs after Cell 3 and processes 'clean_*' tables.

import logging
import time
import os
import re # For filtering tables
from google.cloud import bigquery
from google.api_core import exceptions as google_exceptions # Alias specific exceptions
# *** FIX: Correct import for TableListItem ***
from google.cloud.bigquery.table import TableListItem
from concurrent.futures import ThreadPoolExecutor, as_completed
# from tqdm import tqdm # Optional: Uncomment if tqdm is installed and desired
import random
import hashlib
import json
from datetime import datetime # For timestamp
from typing import Dict, List, Any, Optional, Union, Tuple
from tenacity import retry as tenacity_retry, stop_after_attempt, wait_exponential, retry_if_exception_type as tenacity_if_exception_type


# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s')
logger = logging.getLogger(__name__)
# (Reduce verbosity logs)
logging.getLogger('google.api_core').setLevel(logging.WARNING)
logging.getLogger('google.auth').setLevel(logging.WARNING)
logging.getLogger('google.cloud').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

# --- Configuration ---
PROJECT_ID = os.environ.get('PROJECT_ID', "spry-bus-425315-p6")
DATASET_ID = os.environ.get('DATASET_ID', "mycocoons")
# Table name for the dictionary itself
DATA_DICTIONARY_TABLE_NAME = os.environ.get('DATA_DICTIONARY_TABLE', "data_dictionary")
FULL_DATA_DICTIONARY_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{DATA_DICTIONARY_TABLE_NAME}"
# Pattern to identify tables to include in the dictionary (e.g., cleaned tables from Cell 3)
TABLE_INCLUDE_PATTERN = r'^clean_.*' # Process tables starting with 'clean_'

# --- Retry Configuration ---
RETRYABLE_EXCEPTIONS_TENACITY = (
    google_exceptions.GoogleAPIError, ConnectionError, TimeoutError, google_exceptions.BadRequest
)
def retry_with_backoff_tenacity(retries=3, backoff_in_seconds=1, max_wait=10, retry_exceptions=RETRYABLE_EXCEPTIONS_TENACITY):
    """Returns a tenacity retry decorator with exponential backoff."""
    if not isinstance(retry_exceptions, tuple): retry_exceptions = (retry_exceptions,)
    return tenacity_retry(
        retry=tenacity_if_exception_type(retry_exceptions), stop=stop_after_attempt(retries),
        wait=wait_exponential(multiplier=backoff_in_seconds, min=1, max=max_wait),
        before_sleep=lambda rs: logger.warning(f"Retrying {rs.fn.__name__} due to {rs.outcome.exception()} (attempt {rs.attempt_number})"),
        reraise=True
    )

# --- GCP Clients (SHOULD BE IN SHARED UTILS) ---
bq_client: Optional[bigquery.Client] = None

def initialize_client(max_retries=3):
    """Initialize BigQuery client with retry logic."""
    global bq_client
    if bq_client: return
    logger.info("Attempting to initialize BigQuery client...")
    retry_count = 0
    while retry_count < max_retries:
        try:
            bq_client = bigquery.Client(project=PROJECT_ID)
            bq_client.list_datasets(max_results=1) # Verify connection
            logger.info("Successfully initialized BigQuery client")
            return
        except Exception as e:
            retry_count += 1; wait_time = 2 ** retry_count
            logger.warning(f"Error initializing BQ client: {e}. Retry {retry_count}/{max_retries} in {wait_time}s")
            time.sleep(wait_time)
    logger.error("Failed to initialize BigQuery client after multiple attempts.")
    raise RuntimeError("Failed to initialize BigQuery client")

# --- Service Functions ---

@retry_with_backoff_tenacity()
def create_data_dictionary_table():
    """Creates the data dictionary table if it doesn't exist."""
    if bq_client is None: raise RuntimeError("BigQuery client not initialized.")

    schema = [
        bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("column_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("data_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("mode", "STRING", description="NULLABLE, REQUIRED, REPEATED"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("schema_hash", "STRING", description="MD5 hash of table schema structure"),
        bigquery.SchemaField("last_captured_at", "TIMESTAMP", mode="REQUIRED"),
    ]
    table = bigquery.Table(FULL_DATA_DICTIONARY_TABLE_ID, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(field="last_captured_at", type_="DAY")
    table.clustering_fields = ["table_name"]

    try:
        bq_client.get_table(FULL_DATA_DICTIONARY_TABLE_ID)
        logger.info(f"Data dictionary table {FULL_DATA_DICTIONARY_TABLE_ID} already exists.")
        # Future enhancement: Add schema validation/update for the dictionary table itself if needed.
    except google_exceptions.NotFound:
        logger.info(f"Data dictionary table {FULL_DATA_DICTIONARY_TABLE_ID} not found. Creating...")
        try:
            bq_client.create_table(table)
            logger.info(f"Created data dictionary table {FULL_DATA_DICTIONARY_TABLE_ID}")
        except google_exceptions.Conflict:
            logger.info(f"Data dictionary table {FULL_DATA_DICTIONARY_TABLE_ID} was created by another process.")
        except Exception as e: logger.error(f"Error creating data dictionary table: {e}", exc_info=True); raise
    except Exception as e: logger.error(f"Error checking/creating data dictionary table: {e}", exc_info=True); raise

def calculate_schema_hash(schema: List[bigquery.SchemaField]) -> str:
    """Calculates an MD5 hash of the table schema structure."""
    if not schema: return ""
    # Ensure consistent ordering and representation for hashing
    schema_repr = sorted([
        {"name": field.name, "type": field.field_type, "mode": field.mode or "NULLABLE"} # Default mode
        for field in schema if hasattr(field, 'name') and hasattr(field, 'field_type') # Ensure fields are valid
    ], key=lambda x: x['name'])
    schema_json = json.dumps(schema_repr, sort_keys=True)
    return hashlib.md5(schema_json.encode('utf-8')).hexdigest()

def extract_schema_info(table: bigquery.Table) -> Tuple[List[Dict[str, Any]], str]:
    """Extracts schema information and calculates schema hash from a BigQuery table."""
    table_id = table.table_id
    schema = table.schema
    capture_ts = datetime.utcnow()
    rows_to_insert = []
    if not schema: logger.warning(f"Table {table_id} has no schema defined."); return [], ""

    schema_hash = calculate_schema_hash(schema)
    if not schema_hash: logger.warning(f"Could not calculate schema hash for {table_id}"); return [], ""

    for field in schema:
        if not hasattr(field, 'name') or not hasattr(field, 'field_type'):
             logger.warning(f"Skipping invalid field in table {table_id}: {field}")
             continue
        rows_to_insert.append({
            "table_name": table_id,
            "column_name": field.name,
            "data_type": field.field_type,
            "mode": field.mode or "NULLABLE", # Use NULLABLE if mode is None
            "description": field.description or "", # Use empty string if description is None
            "schema_hash": schema_hash,
            "last_captured_at": capture_ts.isoformat(), # Use ISO format for BQ TIMESTAMP
        })
    return rows_to_insert, schema_hash

@retry_with_backoff_tenacity()
def insert_rows_to_data_dictionary(rows: List[Dict[str, Any]], table_id_processed: str) -> None:
    """Inserts schema rows into the data dictionary table using streaming insert."""
    if bq_client is None: raise RuntimeError("BigQuery client not initialized.")
    if not rows: logger.debug(f"No rows to insert for table {table_id_processed}."); return

    try:
        # Fail fast if any row in the batch is invalid
        errors = bq_client.insert_rows_json(FULL_DATA_DICTIONARY_TABLE_ID, rows, skip_invalid_rows=False)
        if errors:
            error_count = sum(len(e.get('errors', [])) for e in errors)
            logger.error(f"Encountered {error_count} errors inserting rows for {table_id_processed} into data dictionary: {errors}")
            # Raising error ensures retry wrapper catches it
            raise google_exceptions.GoogleAPIError(f"{error_count} errors during data dictionary insert.")
        else:
            logger.debug(f"Successfully inserted {len(rows)} schema rows for table {table_id_processed} into {DATA_DICTIONARY_TABLE_NAME}")
    except Exception as e:
        logger.error(f"Error inserting rows for table {table_id_processed} into data dictionary: {e}", exc_info=True)
        raise # Reraise exception

@retry_with_backoff_tenacity()
def get_existing_schema_hashes() -> Dict[str, str]:
    """Retrieves the latest schema hash for each table currently in the data dictionary."""
    if bq_client is None: raise RuntimeError("BigQuery client not initialized.")
    existing_hashes = {}
    # Query assumes the dictionary table exists (checked/created earlier)
    query = f"""
        SELECT table_name, schema_hash
        FROM `{FULL_DATA_DICTIONARY_TABLE_ID}`
        -- Ensure we only get the very latest hash if multiple entries exist per table
        QUALIFY ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY last_captured_at DESC) = 1
    """
    try:
        logger.info("Fetching existing schema hashes from data dictionary...")
        query_job = bq_client.query(query)
        # Use iterator for potentially large results
        for row in query_job.result(timeout=120): # 2 min timeout
            if row.table_name and row.schema_hash: # Ensure data is valid
                 existing_hashes[row.table_name] = row.schema_hash
        logger.info(f"Retrieved existing schema hashes for {len(existing_hashes)} tables.")
        return existing_hashes
    except google_exceptions.NotFound:
        # This shouldn't happen if create_data_dictionary_table ran successfully
        logger.error(f"Data dictionary table {FULL_DATA_DICTIONARY_TABLE_ID} not found while fetching hashes.")
        raise # Fail if dictionary table missing at this stage
    except Exception as e: logger.error(f"Error retrieving existing schema hashes: {e}", exc_info=True); raise

# @retry_with_backoff_tenacity() # Retries handled internally
def process_table_schema(table_item: TableListItem, existing_hashes: Dict[str, str]) -> Tuple[str, bool, Optional[str]]:
    """Processes a single table: gets schema, compares hash, inserts if new/changed."""
    # *** FIX: Use imported TableListItem for type hint ***
    if bq_client is None: return table_item.table_id, False, "Client not initialized"

    table_id = table_item.table_id
    full_table_id_path = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
    logger.debug(f"Processing schema for table: {table_id}")

    try:
        # Get the full table object to access the schema
        table = bq_client.get_table(full_table_id_path)
        rows_to_insert, current_schema_hash = extract_schema_info(table)

        # Skip if no schema info could be extracted
        if not rows_to_insert or not current_schema_hash:
            logger.warning(f"Skipping table {table_id} due to missing schema or hash calculation error.")
            return table_id, True, "Skipped (No Schema/Hash)" # Treat as success (nothing to do)

        existing_hash = existing_hashes.get(table_id)

        # Compare with existing hash from the dictionary
        if existing_hash == current_schema_hash:
            logger.debug(f"Schema for table {table_id} has not changed (Hash: {current_schema_hash[:8]}...). Skipping insert.")
            return table_id, True, "Skipped (Schema Unchanged)" # Success, no action needed
        else:
            # Schema is new or has changed, insert/overwrite the dictionary entry
            if existing_hash:
                logger.info(f"Schema change detected for table {table_id}. Updating dictionary.")
                # Note: insert_rows_json doesn't update. For true update, would need DELETE+INSERT or MERGE.
                # Given we partition by date and get latest hash, new inserts effectively replace old.
            else:
                logger.info(f"New table {table_id} detected. Adding schema to dictionary.")

            # This uses streaming insert. If Cell 4.2 runs immediately, it might hit conflicts.
            insert_rows_to_data_dictionary(rows_to_insert, table_id)
            return table_id, True, "Updated/Inserted" # Success

    except google_exceptions.NotFound:
        logger.warning(f"Table {table_id} not found during schema processing (may have been deleted). Skipping.")
        return table_id, True, "Skipped (Not Found)" # Treat as success (nothing to do)
    except Exception as e:
        # Catch all other errors during processing for a single table
        error_message = f"Error processing schema for table {table_id}: {e}"
        logger.exception(error_message) # Log full traceback
        return table_id, False, error_message # Failure for this specific table


def process_schemas_in_parallel(max_workers=5):
    """Processes schemas for relevant tables in the dataset in parallel."""
    if bq_client is None: raise RuntimeError("BigQuery client not initialized.")

    processed_count = 0; success_count = 0; skipped_count = 0; failed_count = 0
    start_time = time.time()

    try:
        logger.info(f"Listing tables in dataset {PROJECT_ID}.{DATASET_ID} matching pattern '{TABLE_INCLUDE_PATTERN}'...")
        tables_iterator = bq_client.list_tables(DATASET_ID)
        # Compile regex pattern for filtering
        try:
            pattern = re.compile(TABLE_INCLUDE_PATTERN, re.IGNORECASE)
        except re.error as re_err:
             logger.error(f"Invalid TABLE_INCLUDE_PATTERN regex '{TABLE_INCLUDE_PATTERN}': {re_err}. Cannot proceed.")
             return

        # Filter for actual tables matching the include pattern (e.g., clean_*)
        tables_to_process = [
            table for table in tables_iterator
            if table.table_type == 'TABLE' and pattern.match(table.table_id)
        ]

        if not tables_to_process:
            logger.warning(f"No tables found in dataset {DATASET_ID} matching pattern '{TABLE_INCLUDE_PATTERN}'.")
            return

        total_tables = len(tables_to_process)
        logger.info(f"Found {total_tables} tables matching pattern to process.")

        # Fetch existing hashes once before starting parallel processing
        existing_hashes = get_existing_schema_hashes()

        logger.info(f"Starting parallel schema processing for {total_tables} tables with {max_workers} workers...")
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="schema_worker") as executor:
            futures = {executor.submit(process_table_schema, table, existing_hashes): table.table_id for table in tables_to_process}

            # Optional: Use tqdm for progress bar if installed
            try:
                 from tqdm import tqdm
                 progress_iterator = tqdm(as_completed(futures), total=total_tables, desc="Processing Schemas", unit="table")
            except ImportError:
                 logger.info("tqdm not found, progress bar disabled.")
                 progress_iterator = as_completed(futures)

            for future in progress_iterator:
                table_id = futures[future]
                processed_count += 1
                try:
                    origin_table_id, success, message = future.result()
                    if success:
                        if message and "Skipped" in message: skipped_count += 1
                        else: success_count += 1
                    else: failed_count += 1; logger.error(f"FAILED: {origin_table_id} - {message}")
                except Exception as e:
                    # Catch unexpected errors from the future execution itself
                    failed_count += 1; logger.error(f"FAILED (Future Error): {table_id} - {e}", exc_info=True)

    except Exception as e:
        # Catch errors during setup (listing tables, getting hashes)
        logger.error(f"An error occurred during parallel schema processing setup: {e}", exc_info=True)
    finally:
        end_time = time.time()
        logger.info(f"Finished schema processing in {end_time - start_time:.2f} seconds.")
        logger.info(f"Summary: Processed: {processed_count}, Success/Updated: {success_count}, Skipped: {skipped_count}, Failed: {failed_count}")

def validate_environment():
    """Validates the environment configuration needed for this cell."""
    if not PROJECT_ID or not DATASET_ID or not DATA_DICTIONARY_TABLE_NAME:
        logger.critical("Missing required configuration (PROJECT_ID, DATASET_ID, DATA_DICTIONARY_TABLE). Exiting.")
        return False
    if not TABLE_INCLUDE_PATTERN:
         logger.warning("TABLE_INCLUDE_PATTERN is not set. Will process all tables.")
    return True

def main():
    """Main function to run the data dictionary population."""
    global bq_client
    logger.info("Starting Data Dictionary Population (Cell 4.1)...")
    if not validate_environment(): return

    try:
        initialize_client()
        create_data_dictionary_table()
        process_schemas_in_parallel(max_workers=8) # Slightly increased workers
        logger.info("Data Dictionary Population (Cell 4.1) finished.")
    except Exception as e:
        logger.critical(f"A critical error occurred in the main data dictionary process: {e}", exc_info=True)

if __name__ == "__main__":
    main()
User
Cell In[13], line 320 try: initialize_client() create_data_dictionary_table() process_schemas_in_parallel(max_workers=8) # Slightly increased workers logger.info("Data Dictionary Population (Cell 4.1) finished.") except Exception as e: logger.critical(f"A critical error occurred in the main data dictionary process: {e}", exc_info=True) ---> 334 if name == "main": 335 main()

NameError: name 'name' is not defined

AI
You've found a simple but crucial Python error in the execution block of Cell 4.1.

Error:

NameError: name 'name' is not defined
Location: Cell 4.1, line 334.
Cause: The code uses if name == "main": which attempts to access a variable named name that has not been defined in this scope.
Correct Python Idiom: The standard Python idiom to check if a script is being run directly (as opposed to being imported as a module) uses the special built-in variable __name__ (with double underscores).
Incorrect Line:

if name == "main":
Correct Line:

if __name__ == "__main__":
Solution:

Replace name with __name__ in the final execution block.

Corrected Code Snippet for Cell 4.1 (Execution Block):

# --- Execution ---
if __name__ == "__main__": # <-- FIX: Use double underscores
    logger.info("Starting Data Dictionary Population (Cell 4.1 - Corrected)...")
    main()
    logger.info("Data Dictionary Population (Cell 4.1 - Corrected) finished.")
Action:

Make this one-character change (add the double underscores) in the final lines of your Cell 4.1 code and run it again.

User
2025-04-24 14:19:23,174 - INFO - 1890437957.py:335 - Starting Data Dictionary Population (Cell 4.1 - Corrected)... 2025-04-24 14:19:23,175 - INFO - 1890437957.py:326 - Starting main data dictionary process... 2025-04-24 14:19:23,176 - INFO - 1890437957.py:321 - Environment validation successful. 2025-04-24 14:19:23,177 - INFO - 1890437957.py:48 - Attempting to initialize BigQuery client... 2025-04-24 14:19:23,184 - INFO - 1890437957.py:52 - Successfully initialized BigQuery client 2025-04-24 14:19:23,310 - INFO - 1890437957.py:83 - Data dictionary table spry-bus-425315-p6.mycocoons.data_dictionary already exists. 2025-04-24 14:19:23,311 - INFO - 1890437957.py:275 - Listing tables in dataset spry-bus-425315-p6.mycocoons matching pattern '^clean_.*'... 2025-04-24 14:19:23,459 - WARNING - 1890437957.py:286 - No tables found in dataset mycocoons matching pattern '^clean_.*'. 2025-04-24 14:19:23,460 - INFO - 1890437957.py:316 - Finished schema processing in 0.15 seconds. 2025-04-24 14:19:23,460 - INFO - 1890437957.py:317 - Summary: Processed: 0, Success/Updated: 0, Skipped: 0, Failed: 0 2025-04-24 14:19:23,461 - INFO - 1890437957.py:330 - Data Dictionary Population (Cell 4.1) finished. 2025-04-24 14:19:23,462 - INFO - 1890437957.py:336 - Data Dictionary Population (Cell 4.1 - Corrected) finished.

AI
Okay, the NameError is fixed and Cell 4.1 ran successfully without code errors this time.

Log Analysis:

Initialization: Succeeded.
Data Dictionary Table: Already exists, which is fine.
Table Listing & Filtering:
INFO - Listing tables... matching pattern '^clean_.*'... - It correctly started looking for tables to process.
WARNING - No tables found in dataset mycocoons matching pattern '^clean_.*'. - This is the key outcome. The script searched your dataset but did not find any tables whose names start with clean_.
Processing Summary: Consequently, 0 tables were processed.
Completion: The script finished normally.
Conclusion:

Cell 4.1 is working correctly now in terms of its own logic and execution.
The reason it didn't process anything is that there are currently no tables in your spry-bus-425315-p6.mycocoons dataset that match the pattern ^clean_.* (i.e., start with clean_).
This is expected because Cell 3 (which creates the clean_* tables) hasn't run successfully yet end-to-end. You only ran Cell 3's local test, which failed because its input stg_ table was missing.
Next Steps:

Pipeline Sequence: You need to get the preceding steps working to generate the input for Cell 4.1:
Run Cell 1 (v6): To discover blobs, update the manifest to PENDING, and publish triggers to etl-parsing-requests.
Deploy & Run Cell 2 (v5): Ensure Cell 2 is deployed as a service listening to etl-parsing-requests. When triggered, it should load data into the stg_..._YYYYMMDD tables and publish triggers to etl-cleaning-requests.
Deploy & Run Cell 3 (v4): Ensure Cell 3 is deployed as a service listening to etl-cleaning-requests. When triggered, it should read the stg_... table, clean the data, and load it into the clean_..._YYYYMMDD table.
Run Cell 4.1: Only after Cell 3 has successfully created some clean_* tables can you run Cell 4.1 again. It should then find those tables and populate the data_dictionary.
You are correctly moving through the cells, but remember that for the full pipeline, Cells 2 and 3 need to be deployed as services that react to the Pub/Sub triggers initiated by the previous step, rather than just running their local test blocks.

User
ok now print the final complete code for cel 4.1 and 4.2

AI
Okay, here are the final, complete codes for Cell 4.1 and Cell 4.2, incorporating the previous fixes and ready for execution (assuming Cell 3 has run successfully and created `clean_*` tables).

Remember the relationship:

*   **Cell 4.1** reads `clean_*` tables from BigQuery and populates the `data_dictionary` table in BigQuery.
*   **Cell 4.2** reads the `data_dictionary` table from BigQuery and populates Google Cloud Data Catalog.
*   The remediation report suggested merging these, but they are presented separately here as requested previously. If merging, Cell 4.2's logic would be integrated into Cell 4.1's `process_table_schema` function after the dictionary insert succeeds.

**Cell 4.1: Data Dictionary Population (v3 FINAL)**

```python
# Cell 4.1: Data Dictionary Population (v3 FINAL - Corrected Imports & Execution)
# NOTE: Assumes it runs after Cell 3 and processes 'clean_*' tables.
#       Should ideally use shared utils for clients/retry.

import logging
import time
import os
import re # For filtering tables
from google.cloud import bigquery
from google.api_core import exceptions as google_exceptions # Alias specific exceptions
# Correct import for TableListItem
from google.cloud.bigquery.table import TableListItem
from concurrent.futures import ThreadPoolExecutor, as_completed
# from tqdm import tqdm # Optional: Uncomment if tqdm is installed and desired
import random
import hashlib
import json
from datetime import datetime # For timestamp
from typing import Dict, List, Any, Optional, Union, Tuple
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
# Table name for the dictionary itself
DATA_DICTIONARY_TABLE_NAME = os.environ.get('DATA_DICTIONARY_TABLE', "data_dictionary")
FULL_DATA_DICTIONARY_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{DATA_DICTIONARY_TABLE_NAME}"
# Pattern to identify tables to include in the dictionary (e.g., cleaned tables from Cell 3)
TABLE_INCLUDE_PATTERN = r'^clean_.*' # Process tables starting with 'clean_'

# --- Retry Configuration (SHOULD BE IN SHARED UTILS) ---
RETRYABLE_EXCEPTIONS_TENACITY = (
    google_exceptions.GoogleAPIError, ConnectionError, TimeoutError, google_exceptions.BadRequest
)
def retry_with_backoff_tenacity(retries=3, backoff_in_seconds=1, max_wait=10, retry_exceptions=RETRYABLE_EXCEPTIONS_TENACITY):
    """Returns a tenacity retry decorator with exponential backoff."""
    if not isinstance(retry_exceptions, tuple): retry_exceptions = (retry_exceptions,)
    return tenacity_retry(
        retry=tenacity_if_exception_type(retry_exceptions), stop=stop_after_attempt(retries),
        wait=wait_exponential(multiplier=backoff_in_seconds, min=1, max=max_wait),
        before_sleep=lambda rs: logger.warning(f"Retrying {rs.fn.__name__} due to {rs.outcome.exception()} (attempt {rs.attempt_number})"),
        reraise=True
    )

# --- GCP Clients (SHOULD BE IN SHARED UTILS) ---
bq_client: Optional[bigquery.Client] = None

def initialize_client(max_retries=3):
    """Initialize BigQuery client with retry logic."""
    global bq_client
    if bq_client: return
    logger.info("Attempting to initialize BigQuery client...")
    retry_count = 0
    while retry_count < max_retries:
        try:
            bq_client = bigquery.Client(project=PROJECT_ID)
            bq_client.list_datasets(max_results=1) # Verify connection
            logger.info("Successfully initialized BigQuery client")
            return
        except Exception as e:
            retry_count += 1; wait_time = 2 ** retry_count
            logger.warning(f"Error initializing BigQuery client: {e}. Retrying in {wait_time} seconds. Attempt {retry_count}/{max_retries}")
            time.sleep(wait_time)
    logger.error("Failed to initialize BigQuery client after multiple attempts.")
    raise RuntimeError("Failed to initialize BigQuery client")

# --- Service Functions ---

@retry_with_backoff_tenacity()
def create_data_dictionary_table():
    """Creates the data dictionary table if it doesn't exist."""
    if bq_client is None: raise RuntimeError("BigQuery client not initialized.")

    schema = [
        bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("column_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("data_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("mode", "STRING", description="NULLABLE, REQUIRED, REPEATED"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("schema_hash", "STRING", description="MD5 hash of table schema structure"),
        bigquery.SchemaField("last_captured_at", "TIMESTAMP", mode="REQUIRED"),
    ]
    table = bigquery.Table(FULL_DATA_DICTIONARY_TABLE_ID, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(field="last_captured_at", type_="DAY")
    table.clustering_fields = ["table_name"]

    try:
        bq_client.get_table(FULL_DATA_DICTIONARY_TABLE_ID)
        logger.info(f"Data dictionary table {FULL_DATA_DICTIONARY_TABLE_ID} already exists.")
    except google_exceptions.NotFound:
        logger.info(f"Data dictionary table {FULL_DATA_DICTIONARY_TABLE_ID} not found. Creating...")
        try:
            bq_client.create_table(table)
            logger.info(f"Created data dictionary table {FULL_DATA_DICTIONARY_TABLE_ID}")
        except google_exceptions.Conflict:
            logger.info(f"Data dictionary table {FULL_DATA_DICTIONARY_TABLE_ID} created concurrently.")
        except Exception as e: logger.error(f"Error creating data dictionary table: {e}", exc_info=True); raise
    except Exception as e: logger.error(f"Error checking/creating data dictionary table: {e}", exc_info=True); raise

def calculate_schema_hash(schema: List[bigquery.SchemaField]) -> str:
    """Calculates an MD5 hash of the table schema structure."""
    if not schema: return ""
    schema_repr = sorted([
        {"name": field.name, "type": field.field_type, "mode": field.mode or "NULLABLE"}
        for field in schema if hasattr(field, 'name') and hasattr(field, 'field_type')
    ], key=lambda x: x['name'])
    schema_json = json.dumps(schema_repr, sort_keys=True)
    return hashlib.md5(schema_json.encode('utf-8')).hexdigest()

def extract_schema_info(table: bigquery.Table) -> Tuple[List[Dict[str, Any]], str]:
    """Extracts schema information and calculates schema hash from a BigQuery table."""
    table_id = table.table_id; schema = table.schema; capture_ts = datetime.utcnow()
    rows_to_insert = [];
    if not schema: logger.warning(f"Table {table_id} has no schema."); return [], ""
    schema_hash = calculate_schema_hash(schema)
    if not schema_hash: logger.warning(f"Cannot calc schema hash for {table_id}"); return [], ""
    for field in schema:
        if not hasattr(field, 'name') or not hasattr(field, 'field_type'): continue
        rows_to_insert.append({
            "table_name": table_id, "column_name": field.name,
            "data_type": field.field_type, "mode": field.mode or "NULLABLE",
            "description": field.description or "", "schema_hash": schema_hash,
            "last_captured_at": capture_ts.isoformat(),
        })
    return rows_to_insert, schema_hash

@retry_with_backoff_tenacity()
def insert_rows_to_data_dictionary(rows: List[Dict[str, Any]], table_id_processed: str) -> None:
    """Inserts schema rows into the data dictionary table using streaming insert."""
    if bq_client is None: raise RuntimeError("BigQuery client not initialized.")
    if not rows: logger.debug(f"No rows to insert for {table_id_processed}."); return
    try:
        errors = bq_client.insert_rows_json(FULL_DATA_DICTIONARY_TABLE_ID, rows, skip_invalid_rows=False)
        if errors:
            error_count = sum(len(e.get('errors', [])) for e in errors)
            logger.error(f"Dict insert errors for {table_id_processed}: {errors}")
            raise google_exceptions.GoogleAPIError(f"{error_count} errors during data dictionary insert.")
        else:
            logger.debug(f"Inserted {len(rows)} rows for {table_id_processed} into {DATA_DICTIONARY_TABLE_NAME}")
    except Exception as e:
        logger.error(f"Error inserting rows for {table_id_processed}: {e}", exc_info=True)
        raise

@retry_with_backoff_tenacity()
def get_existing_schema_hashes() -> Dict[str, str]:
    """Retrieves the latest schema hash for each table currently in the data dictionary."""
    if bq_client is None: raise RuntimeError("BigQuery client not initialized.")
    existing_hashes = {}
    query = f"""
        SELECT table_name, schema_hash
        FROM `{FULL_DATA_DICTIONARY_TABLE_ID}`
        QUALIFY ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY last_captured_at DESC) = 1
    """
    try:
        logger.info("Fetching existing schema hashes from data dictionary...")
        query_job = bq_client.query(query)
        for row in query_job.result(timeout=120):
            if row.table_name and row.schema_hash: existing_hashes[row.table_name] = row.schema_hash
        logger.info(f"Retrieved {len(existing_hashes)} existing schema hashes.")
        return existing_hashes
    except google_exceptions.NotFound:
        logger.error(f"Data dictionary table {FULL_DATA_DICTIONARY_TABLE_ID} not found."); raise
    except Exception as e: logger.error(f"Error retrieving existing schema hashes: {e}", exc_info=True); raise

# @retry_with_backoff_tenacity() # Retries handled internally
def process_table_schema(table_item: TableListItem, existing_hashes: Dict[str, str]) -> Tuple[str, bool, Optional[str]]:
    """Processes a single table: gets schema, compares hash, inserts if new/changed."""
    if bq_client is None: return table_item.table_id, False, "Client not initialized"

    table_id = table_item.table_id
    full_table_id_path = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
    logger.debug(f"Processing schema for table: {table_id}")
    try:
        table = bq_client.get_table(full_table_id_path)
        rows_to_insert, current_schema_hash = extract_schema_info(table)
        if not rows_to_insert or not current_schema_hash:
            return table_id, True, "Skipped (No Schema/Hash)"

        existing_hash = existing_hashes.get(table_id)
        if existing_hash == current_schema_hash:
            logger.debug(f"Schema unchanged for {table_id}. Skipping.")
            return table_id, True, "Skipped (Schema Unchanged)"
        else:
            if existing_hash: logger.info(f"Schema change detected for {table_id}. Updating dictionary.")
            else: logger.info(f"New table {table_id} detected. Adding schema to dictionary.")
            insert_rows_to_data_dictionary(rows_to_insert, table_id)
            return table_id, True, "Updated/Inserted"

    except google_exceptions.NotFound:
        logger.warning(f"Table {table_id} not found during processing. Skipping.")
        return table_id, True, "Skipped (Not Found)"
    except Exception as e:
        error_message = f"Error processing schema for {table_id}: {e}"
        logger.exception(error_message)
        return table_id, False, error_message

def process_schemas_in_parallel(max_workers=5):
    """Processes schemas for relevant tables in the dataset in parallel."""
    if bq_client is None: raise RuntimeError("BigQuery client not initialized.")
    processed_count = 0; success_count = 0; skipped_count = 0; failed_count = 0
    start_time = time.time()
    try:
        logger.info(f"Listing tables in {PROJECT_ID}.{DATASET_ID} matching '{TABLE_INCLUDE_PATTERN}'...")
        tables_iterator = bq_client.list_tables(DATASET_ID)
        try: pattern = re.compile(TABLE_INCLUDE_PATTERN, re.IGNORECASE)
        except re.error as re_err: logger.error(f"Invalid regex '{TABLE_INCLUDE_PATTERN}': {re_err}"); return
        tables_to_process = [t for t in tables_iterator if t.table_type == 'TABLE' and pattern.match(t.table_id)]
        if not tables_to_process: logger.warning(f"No tables found matching pattern."); return
        total_tables = len(tables_to_process); logger.info(f"Found {total_tables} tables to process.")

        existing_hashes = get_existing_schema_hashes()
        logger.info(f"Processing {total_tables} schemas with {max_workers} workers...")
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="schema_worker") as executor:
            futures = {executor.submit(process_table_schema, table, existing_hashes): table.table_id for table in tables_to_process}
            # Use tqdm if available
            try: from tqdm import tqdm; progress_iterator = tqdm(as_completed(futures), total=total_tables, desc="Processing Schemas", unit="table")
            except ImportError: logger.info("tqdm not found, progress bar disabled."); progress_iterator = as_completed(futures)
            for future in progress_iterator:
                table_id = futures[future]; processed_count += 1
                try:
                    _, success, message = future.result()
                    if success:
                        if message and "Skipped" in message: skipped_count += 1
                        else: success_count += 1
                    else: failed_count += 1; logger.error(f"FAILED Schema: {table_id} - {message}")
                except Exception as e: failed_count += 1; logger.error(f"FAILED Future: {table_id} - {e}", exc_info=True)
    except Exception as e: logger.error(f"Schema processing setup error: {e}", exc_info=True)
    finally:
        end_time = time.time(); logger.info(f"Schema processing finished in {end_time - start_time:.2f}s.")
        logger.info(f"Summary: Processed: {processed_count}, Success/Updated: {success_count}, Skipped: {skipped_count}, Failed: {failed_count}")

def validate_environment():
    """Validates configuration."""
    if not PROJECT_ID or not DATASET_ID or not DATA_DICTIONARY_TABLE_NAME:
        logger.critical("Missing required config (PROJECT_ID, DATASET_ID, DATA_DICTIONARY_TABLE_NAME)."); return False
    return True

def main():
    """Main function to run the data dictionary population."""
    global bq_client
    logger.info("Starting Data Dictionary Population (Cell 4.1)...")
    if not validate_environment(): return
    try:
        initialize_client()
        create_data_dictionary_table()
        process_schemas_in_parallel(max_workers=8)
        logger.info("Data Dictionary Population (Cell 4.1) finished.")
    except Exception as e:
        logger.critical(f"Data dictionary process critical error: {e}", exc_info=True)

# --- Execution ---
if __name__ == "__main__":
    logger.info("Starting Data Dictionary Population (Cell 4.1 - Corrected v3)...")
    main()
    logger.info("Data Dictionary Population (Cell 4.1 - Corrected v3) finished.")
```


      Cell In[5], line 341
        You've found a simple but crucial Python error in the execution block of Cell 4.1.
           ^
    SyntaxError: unterminated string literal (detected at line 341)




```python
# Cell 4.2: Data Catalog Integration (Fixed v4 - Corrected Import)
# NOTE: Remediation Report suggests merging this with 4.1. This version is standalone.
#       Assumes it runs after Cell 4.1 and reads from the data_dictionary table.

import logging
import re
import hashlib
import os
import time
import random
from google.cloud import bigquery
# *** Corrected Data Catalog imports ***
from google.cloud.datacatalog_v1 import DataCatalogClient, types
from google.api_core import exceptions as google_exceptions # Alias specific exceptions
from concurrent.futures import ThreadPoolExecutor, as_completed
# from tqdm import tqdm # Optional: Uncomment if tqdm is installed
from typing import Dict, List, Any, Optional, Union, Tuple
from collections import defaultdict
from tenacity import retry as tenacity_retry, stop_after_attempt, wait_exponential, retry_if_exception_type as tenacity_if_exception_type

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
logger = logging.getLogger(__name__)
# (Reduce verbosity logs)
logging.getLogger('google.api_core').setLevel(logging.WARNING); logging.getLogger('google.auth').setLevel(logging.WARNING)
logging.getLogger('google.cloud').setLevel(logging.WARNING); logging.getLogger('urllib3').setLevel(logging.WARNING)

# --- Configuration ---
PROJECT_ID = os.environ.get('PROJECT_ID', "spry-bus-425315-p6")
LOCATION = os.environ.get('LOCATION', "us-central1") # Data Catalog region
DATASET_ID = os.environ.get('DATASET_ID', "mycocoons") # BQ Dataset
ENTRY_GROUP_ID = os.environ.get('ENTRY_GROUP_ID', "mycocoons_data_dictionary")
DATA_DICTIONARY_TABLE_NAME = os.environ.get('DATA_DICTIONARY_TABLE', "data_dictionary")
FULL_DATA_DICTIONARY_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{DATA_DICTIONARY_TABLE_NAME}"

# --- Retry Configuration (SHOULD BE IN SHARED UTILS) ---
RETRYABLE_EXCEPTIONS_TENACITY = (
    google_exceptions.GoogleAPIError, ConnectionError, TimeoutError,
    google_exceptions.ServiceUnavailable, google_exceptions.DeadlineExceeded
)
def retry_with_backoff_tenacity(retries=3, backoff_in_seconds=1, max_wait=10, retry_exceptions=RETRYABLE_EXCEPTIONS_TENACITY):
    """Returns a tenacity retry decorator with exponential backoff."""
    if not isinstance(retry_exceptions, tuple): retry_exceptions = (retry_exceptions,)
    return tenacity_retry(
        retry=tenacity_if_exception_type(retry_exceptions), stop=stop_after_attempt(retries),
        wait=wait_exponential(multiplier=backoff_in_seconds, min=1, max=max_wait),
        before_sleep=lambda rs: logger.warning(f"Retrying {getattr(rs.fn, '__name__', 'function')} due to {rs.outcome.exception()} (attempt {rs.attempt_number})"),
        reraise=True
    )

# --- GCP Clients (SHOULD BE IN SHARED UTILS) ---
datacatalog_client: Optional[DataCatalogClient] = None
bq_client: Optional[bigquery.Client] = None

def initialize_clients(max_retries=3):
    """Initialize GCP clients (Data Catalog, BigQuery) with retry logic."""
    global datacatalog_client, bq_client
    if datacatalog_client and bq_client: return
    logger.info("Attempting to initialize GCP clients (Data Catalog, BigQuery)...")
    retry_count=0; last_exception=None
    while retry_count < max_retries:
        try:
            if not datacatalog_client:
                datacatalog_client = DataCatalogClient()
                logger.debug(f"Testing DC connection: projects/{PROJECT_ID}/locations/{LOCATION}")
                try: _ = list(datacatalog_client.list_entry_groups(parent=f"projects/{PROJECT_ID}/locations/{LOCATION}"))
                except google_exceptions.NotFound: logger.debug("No entry groups found (OK).")
                logger.info("Data Catalog client initialized.")
            if not bq_client:
                bq_client = bigquery.Client(project=PROJECT_ID)
                logger.debug("Testing BQ connection."); _ = list(bq_client.list_datasets(max_results=1))
                logger.info("BigQuery client initialized.")
            if datacatalog_client and bq_client: logger.info("Clients init OK."); return
        except Exception as e:
            last_exception=e; retry_count+=1; wait_time=2**retry_count+random.uniform(0,1)
            logger.warning(f"Client init error: {e}. Retry {retry_count}/{max_retries} in {wait_time:.2f}s", exc_info=False)
            datacatalog_client=None; bq_client=None
            if retry_count >= max_retries: logger.error(f"Client init failed after {max_retries} attempts: {last_exception}", exc_info=True); raise RuntimeError(f"Client init failed") from last_exception
            time.sleep(wait_time)

# --- Service Functions ---

def validate_configuration():
    """Validates the configuration values needed for this cell."""
    missing = [var for var in ['PROJECT_ID', 'LOCATION', 'DATASET_ID', 'ENTRY_GROUP_ID', 'DATA_DICTIONARY_TABLE_NAME'] if not globals().get(var)]
    if missing: raise ValueError(f"Missing required configuration: {', '.join(missing)}")
    logger.info(f"Config validated. Project:{PROJECT_ID}, Location:{LOCATION}, Dataset:{DATASET_ID}, Group:{ENTRY_GROUP_ID}")

def sanitize_entry_id(name: str, max_length: int = 64) -> str:
    """Sanitizes a string to be a valid Data Catalog Entry ID."""
    # (Implementation remains the same as v2)
    if not name: return f"invalid_name_{hashlib.md5(str(random.random()).encode()).hexdigest()[:8]}"
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name).strip('_')
    if sanitized and sanitized[0].isdigit(): sanitized = "_" + sanitized
    if len(sanitized) > max_length:
        name_hash = hashlib.md5(name.encode()).hexdigest()[:8]; prefix_len = max_length - len(name_hash) - 1
        prefix = sanitized[:prefix_len].strip('_'); sanitized = f"{prefix}_{name_hash}"[:max_length]
    if not sanitized: return f"sanitized_empty_{hashlib.md5(name.encode()).hexdigest()[:8]}"
    return sanitized[:max_length]

@retry_with_backoff_tenacity()
def create_or_get_entry_group() -> Optional[str]:
    """Creates or gets the Data Catalog Entry Group. Returns the resource name."""
    # (Implementation remains the same as v2 - using imported types.EntryGroup)
    if datacatalog_client is None: raise RuntimeError("DC client not initialized.")
    entry_group_path = datacatalog_client.entry_group_path(PROJECT_ID,LOCATION,ENTRY_GROUP_ID); parent_path = f"projects/{PROJECT_ID}/locations/{LOCATION}"
    try: logger.debug(f"Checking entry group: {entry_group_path}"); entry_group = datacatalog_client.get_entry_group(name=entry_group_path); logger.info(f"Using existing DC Group: {entry_group.name}"); return entry_group.name
    except google_exceptions.NotFound:
        logger.info(f"Creating entry group '{ENTRY_GROUP_ID}' in {LOCATION}...");
        try: entry_group_obj = types.EntryGroup(); entry_group_obj.display_name=f"{DATASET_ID} Data Dictionary"; entry_group_obj.description=f"Custom DC entries from '{DATA_DICTIONARY_TABLE_NAME}' for BQ dataset '{DATASET_ID}'."; created_entry_group = datacatalog_client.create_entry_group(parent=parent_path, entry_group_id=ENTRY_GROUP_ID, entry_group=entry_group_obj); logger.info(f"Created DC Group: {created_entry_group.name}"); return created_entry_group.name
        except google_exceptions.PermissionDenied as e: logger.error(f"Permission denied creating group '{ENTRY_GROUP_ID}'. Check IAM roles. Err: {e}", exc_info=True); raise
        except google_exceptions.AlreadyExists: logger.warning(f"Group '{ENTRY_GROUP_ID}' created concurrently. Fetching."); time.sleep(random.uniform(0.5, 1.5)); entry_group = datacatalog_client.get_entry_group(name=entry_group_path); logger.info(f"Fetched concurrent group: {entry_group.name}"); return entry_group.name
        except Exception as e: logger.error(f"Error creating group '{ENTRY_GROUP_ID}': {e}", exc_info=True); raise
    except google_exceptions.PermissionDenied as e: logger.error(f"Permission denied accessing group '{ENTRY_GROUP_ID}'. Check IAM roles. Err: {e}", exc_info=True); raise
    except Exception as e: logger.error(f"Error accessing group '{ENTRY_GROUP_ID}': {e}", exc_info=True); raise

def validate_entry_data(table_name: str, columns: List[Dict[str, Any]]) -> bool:
    """Validates the data before attempting to create/update a Data Catalog entry."""
    # (Implementation remains the same as v2)
    if not table_name: logger.warning("Skip entry: Table name missing."); return False
    if not columns: logger.warning(f"Skip entry '{table_name}': No column data."); return False
    valid = True
    for i, col in enumerate(columns):
        col_name=col.get("column_name"); data_type=col.get("data_type")
        if not col_name or not isinstance(col_name, str): logger.warning(f"Invalid col data '{table_name}' idx {i}: 'column_name'. Data: {col}"); valid=False
        if not data_type or not isinstance(data_type, str): logger.warning(f"Invalid col data '{table_name}' idx {i}: 'data_type'. Data: {col}"); valid=False
    if not valid: logger.warning(f"Skipping entry creation '{table_name}': invalid column data.")
    return valid

@retry_with_backoff_tenacity()
def create_or_update_catalog_entry(entry_group_name: str, table_name: str, columns: List[Dict[str, Any]]) -> Tuple[str, bool, Optional[str]]:
    """Creates or updates a Data Catalog entry for a single table."""
    # (Implementation remains the same as v2 - using imported types)
    if datacatalog_client is None: return table_name, False, "DC client not initialized"
    if not entry_group_name: return table_name, False, "Entry group name missing"
    if not validate_entry_data(table_name, columns): return table_name, True, "Skipped (Invalid Input)"

    entry_id = sanitize_entry_id(table_name); entry_path = datacatalog_client.entry_path(PROJECT_ID,LOCATION,ENTRY_GROUP_ID,entry_id)
    linked_resource = f"//bigquery.googleapis.com/projects/{PROJECT_ID}/datasets/{DATASET_ID}/tables/{table_name}"
    catalog_columns = [types.ColumnSchema(column=c["column_name"], type_=c["data_type"], description=c.get("description","")or"", mode=c.get("mode","NULLABLE")or"NULLABLE") for c in columns]

    entry = types.Entry(); entry.display_name=table_name; entry.description=f"Schema for BQ table: {table_name} (Source: {DATA_DICTIONARY_TABLE_NAME})"; entry.linked_resource=linked_resource; entry.schema_=types.Schema(columns=catalog_columns); entry.type_=types.EntryType.TABLE; entry.bigquery_table_spec=types.BigQueryTableSpec(table_source_type=types.TableSourceType.BIGQUERY_TABLE)

    try: logger.debug(f"Getting existing DC entry: {entry_path}"); existing_entry=datacatalog_client.get_entry(name=entry_path); logger.info(f"Updating DC entry for {table_name} (ID: {entry_id})."); entry.name=entry_path; update_mask={"paths":["schema","description","display_name","linked_resource","bigquery_table_spec"]}; updated_entry=datacatalog_client.update_entry(entry=entry,update_mask=update_mask); logger.info(f"Update OK DC entry for {table_name}"); return table_name, True, "Updated"
    except google_exceptions.NotFound:
        logger.info(f"Creating new DC entry for {table_name} (ID: {entry_id})")
        try: created_entry=datacatalog_client.create_entry(parent=entry_group_name, entry_id=entry_id, entry=entry); logger.info(f"Create OK DC entry: {created_entry.name}"); return table_name, True, "Created"
        except google_exceptions.AlreadyExists: logger.warning(f"DC entry {entry_id} created concurrently. Skipping."); return table_name, True, "Skipped (Concurrent Creation)"
        except Exception as create_err: error_message=f"Error creating DC entry {entry_id}: {create_err}"; logger.error(error_message,exc_info=True); return table_name, False, error_message
    except Exception as e: error_message=f"Error processing DC entry {entry_id}: {e}"; logger.error(error_message,exc_info=True); return table_name, False, error_message

@retry_with_backoff_tenacity(retries=5, backoff_in_seconds=2, max_wait=30)
def read_data_dictionary() -> Dict[str, List[Dict[str, Any]]]:
    """Reads the data dictionary table from BigQuery and groups by table name."""
    # (Implementation remains the same as v2 - removed mode query)
    if bq_client is None: raise RuntimeError("BQ client not initialized.")
    query = f"""WITH RankedSchemas AS (SELECT table_name, column_name, data_type, IFNULL(description,'') as description, last_captured_at, ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY last_captured_at DESC) as rn FROM `{FULL_DATA_DICTIONARY_TABLE_ID}` WHERE table_name IS NOT NULL AND column_name IS NOT NULL AND data_type IS NOT NULL) SELECT table_name, column_name, data_type, description, last_captured_at FROM RankedSchemas WHERE rn = 1 ORDER BY table_name, column_name"""
    logger.info(f"Reading data dictionary from: {FULL_DATA_DICTIONARY_TABLE_ID}")
    tables_data = defaultdict(list); processed_rows=0
    try:
        query_job=bq_client.query(query); results=query_job.result(timeout=300)
        for row in results:
            if not row.table_name or not row.column_name or not row.data_type: logger.warning(f"Skip invalid row: {row}"); continue
            processed_rows+=1; tables_data[row.table_name].append({"column_name": row.column_name, "data_type": row.data_type, "description": row.description})
        if processed_rows==0: logger.warning(f"No valid rows found in {FULL_DATA_DICTIONARY_TABLE_ID}.")
        else: logger.info(f"Read schema info for {len(tables_data)} tables ({processed_rows} cols) from dict.")
        return dict(tables_data)
    except google_exceptions.NotFound: logger.error(f"Data dict table {FULL_DATA_DICTIONARY_TABLE_ID} not found."); return {}
    except google_exceptions.BadRequest as bq_err: logger.error(f"Invalid BQ query for {FULL_DATA_DICTIONARY_TABLE_ID}. Check schema/query. Err: {bq_err}",exc_info=True); raise RuntimeError(f"BQ query failed: {bq_err}") from bq_err
    except Exception as e: logger.error(f"Error reading data dict {FULL_DATA_DICTIONARY_TABLE_ID}: {e}", exc_info=True); raise

def process_catalog_entries_in_parallel(entry_group_name: str, tables_data: Dict[str, List[Dict[str, Any]]], max_workers=5):
    """Processes Data Catalog entries in parallel using ThreadPoolExecutor."""
    # (Implementation remains the same as v2)
    if not entry_group_name: logger.error("Cannot process catalog entries: entry group name missing."); return {'success': 0, 'skipped': 0, 'failed': 0}
    if not tables_data: logger.info("No table data from dictionary to process for Data Catalog."); return {'success': 0, 'skipped': 0, 'failed': 0}
    total_tables = len(tables_data); results = {'success': 0, 'skipped': 0, 'failed': 0}; start_time = time.time()
    logger.info(f"Starting parallel DC processing for {total_tables} tables (Workers: {max_workers})...")
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="catalog_worker") as executor:
        futures = { executor.submit(create_or_update_catalog_entry, entry_group_name, table_name, columns): table_name for table_name, columns in tables_data.items() }
        # Optional tqdm progress bar
        try: from tqdm import tqdm; progress_iterator = tqdm(as_completed(futures), total=total_tables, desc="Processing Catalog Entries", unit="table")
        except ImportError: logger.info("tqdm not found, progress bar disabled."); progress_iterator = as_completed(futures)
        for future in progress_iterator:
            table_name = futures[future]
            try: _, success, message = future.result()
            except Exception as e: success=False; message=f"Future Error: {e}"; logger.error(f"FAILED Future: {table_name} - {e}", exc_info=True)
            if success:
                if message and "Skipped" in message: results['skipped'] += 1; logger.debug(f"Skipped {table_name}: {message}")
                else: results['success'] += 1
            else: results['failed'] += 1; # Error logged in create_or_update_catalog_entry or above
    end_time = time.time(); logger.info(f"Finished DC processing in {end_time - start_time:.2f}s.")
    logger.info(f"Summary: Total: {total_tables}, Succeeded: {results['success']}, Skipped: {results['skipped']}, Failed: {results['failed']}")
    return results


def validate_environment():
    """Validates configuration."""
    # (Implementation remains the same as v2)
    missing = [var for var in ['PROJECT_ID', 'LOCATION', 'DATASET_ID', 'ENTRY_GROUP_ID', 'DATA_DICTIONARY_TABLE_NAME'] if not globals().get(var)]
    if missing: raise ValueError(f"Missing required configuration: {', '.join(missing)}")
    logger.info(f"Config validated.")
    return True

def main(dry_run=False):
    """Main function to run the Data Catalog integration."""
    global datacatalog_client, bq_client # Ensure clients are accessible
    start_time = time.time(); logger.info(f"Starting DC Integration (Run: {time.strftime('%Y-%m-%d %H:%M:%S %Z')})...")
    final_status = "UNKNOWN"
    try:
        if not validate_environment(): return
        initialize_clients()
        entry_group_name = create_or_get_entry_group()
        if not entry_group_name: raise RuntimeError("Failed to create/get DC Entry Group.")
        tables_data = read_data_dictionary()
        if not tables_data: logger.warning("No data from dictionary. Exiting."); final_status="COMPLETED (No Data)"; return
        if dry_run:
            logger.warning("--- DRY RUN MODE ENABLED ---"); logger.info(f"Would process {len(tables_data)} tables into {entry_group_name}"); final_status="COMPLETED (Dry Run)"
            # Log example data if available
            if tables_data: first_table=next(iter(tables_data)); logger.info(f"Example Table: {first_table}"); [logger.info(f" - {col}") for col in tables_data[first_table][:5]]
        else:
            logger.info(f"Processing {len(tables_data)} tables into {entry_group_name}")
            results = process_catalog_entries_in_parallel(entry_group_name, tables_data, max_workers=8)
            if results['failed'] > 0: final_status = "COMPLETED WITH ERRORS"
            elif results['success'] > 0 or results['skipped'] > 0: final_status = "COMPLETED SUCCESSFULLY"
            else: final_status = "COMPLETED (No Entries Processed)"
    except Exception as e: final_status = "FAILED"; logger.critical(f"DC integration critical error: {e}", exc_info=True);
    finally: end_time = time.time(); logger.info(f"DC Integration finished. Status: {final_status}. Time: {end_time - start_time:.2f}s.")

if __name__ == "__main__":
    # Run the main function, set dry_run=True for testing without modifying Data Catalog
    main(dry_run=False)
```


```python
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

```

    2025-04-26 18:49:36,192 - WARNING - 2698362772:26 - fuzzywuzzy library not found. Using simple substring matching for table patterns.
    2025-04-26 18:49:36,196 - INFO - 2698362772:92 - Loaded funnel configuration from ./funnel_config.json
    2025-04-26 18:49:36,197 - INFO - 2698362772:126 - Created event-to-stage map with 16 event mappings.
    2025-04-26 18:49:36,207 - INFO - 2698362772:826 - Starting Funnel Analysis and Knowledge Graph Creation (Cell 5)...
    2025-04-26 18:49:36,214 - INFO - 2698362772:146 - Successfully initialized BigQuery client
    2025-04-26 18:49:36,215 - INFO - 2698362772:211 - Discovering tables in dataset mycocoons matching patterns...
    2025-04-26 18:50:26,703 - INFO - 2698362772:223 - Found 18247 tables in dataset mycocoons. Applying patterns...
    2025-04-26 18:50:26,803 - INFO - 2698362772:267 - Discovered 821 tables for type 'shopify_orders'.
    2025-04-26 18:50:26,804 - INFO - 2698362772:267 - Discovered 2018 tables for type 'google_analytics'.
    2025-04-26 18:50:26,805 - INFO - 2698362772:267 - Discovered 4607 tables for type 'facebook_ads'.
    2025-04-26 18:50:26,806 - INFO - 2698362772:267 - Discovered 550 tables for type 'google_ads'.
    2025-04-26 18:50:26,807 - INFO - 2698362772:267 - Discovered 303 tables for type 'ga4_custom'.
    2025-04-26 18:50:26,808 - INFO - 2698362772:271 - Total relevant tables discovered: 8299
    2025-04-26 18:50:26,817 - INFO - 2698362772:850 - Processing 8299 discovered tables for KG/Funnel data...
    2025-04-26 18:50:27,212 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220105_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:27,213 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220101_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:27,213 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220104_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:27,219 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220102_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:27,220 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220103_cleaned. Funnel mapping might fail.
    Reading BQ Tables:   0%|          | 0/8299 [00:00<?, ?it/s]2025-04-26 18:50:27,705 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220105_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:27,706 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:27,709 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220105_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   0%|          | 1/8299 [00:00<1:05:02,  2.13it/s]2025-04-26 18:50:27,736 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220102_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:27,737 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:27,741 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220102_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:27,743 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220101_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:27,744 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:27,747 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220101_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:27,762 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220103_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:27,763 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:27,766 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220103_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:27,770 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220104_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:27,771 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:27,774 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220104_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:27,805 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220106_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:27,838 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220107_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:27,852 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220108_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:27,881 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220109_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:27,887 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220110_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:28,273 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220107_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:28,275 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:28,278 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220107_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   0%|          | 6/8299 [00:01<21:45,  6.35it/s]  2025-04-26 18:50:28,295 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220106_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:28,296 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:28,300 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220106_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:28,301 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220109_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:28,305 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:28,308 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220109_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:28,387 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220108_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:28,388 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:28,391 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220108_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   0%|          | 9/8299 [00:01<14:38,  9.44it/s]2025-04-26 18:50:28,409 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220111_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:28,414 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220110_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:28,416 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:28,419 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220110_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:28,420 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220112_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:28,427 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220113_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:28,499 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220114_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:28,551 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220115_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:28,829 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220111_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:28,830 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:28,833 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220111_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   0%|          | 11/8299 [00:01<19:14,  7.18it/s]2025-04-26 18:50:28,844 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220113_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:28,845 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:28,848 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220113_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:28,866 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220112_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:28,867 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:28,870 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220112_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:28,925 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220117_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:28,932 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220116_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:28,955 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220118_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:29,003 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220115_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:29,004 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:29,006 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220115_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   0%|          | 14/8299 [00:01<14:58,  9.22it/s]2025-04-26 18:50:29,010 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220114_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:29,012 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:29,015 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220114_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:29,125 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220120_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:29,130 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220119_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:29,403 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220118_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:29,405 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:29,406 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220117_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:29,408 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220118_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   0%|          | 16/8299 [00:02<18:22,  7.51it/s]2025-04-26 18:50:29,410 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:29,416 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220117_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:29,441 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220116_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:29,442 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:29,443 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220116_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:29,496 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220121_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:29,521 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220122_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:29,555 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220119_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:29,556 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:29,559 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220119_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   0%|          | 19/8299 [00:02<14:11,  9.73it/s]2025-04-26 18:50:29,560 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220123_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:29,616 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220120_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:29,617 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:29,619 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220120_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:29,679 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220124_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:29,740 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220125_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:29,909 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220121_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:29,910 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:29,913 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220121_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   0%|          | 21/8299 [00:02<16:48,  8.21it/s]2025-04-26 18:50:29,928 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220122_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:29,929 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:29,930 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220122_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:30,015 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220127_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:30,024 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220123_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:30,025 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:30,028 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220123_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   0%|          | 23/8299 [00:02<14:25,  9.57it/s]2025-04-26 18:50:30,068 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220126_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:30,098 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220124_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:30,099 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:30,101 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220124_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:30,123 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220128_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:30,162 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220125_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:30,164 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:30,167 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220125_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   0%|          | 25/8299 [00:02<13:03, 10.56it/s]2025-04-26 18:50:30,215 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220129_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:30,265 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220130_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:30,503 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220126_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:30,505 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:30,508 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220126_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:30,544 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220127_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:30,545 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220128_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:30,546 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:30,547 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:30,551 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220127_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   0%|          | 27/8299 [00:03<16:57,  8.13it/s]2025-04-26 18:50:30,558 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220128_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:30,634 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220131_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:30,644 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220129_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:30,645 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:30,649 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220201_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:30,650 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220129_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:30,677 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220130_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:30,678 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:30,681 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220130_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   0%|          | 30/8299 [00:03<12:44, 10.82it/s]2025-04-26 18:50:30,687 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220202_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:30,770 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220203_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:30,774 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220204_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:31,023 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220131_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:31,024 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:31,027 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220131_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:31,100 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220202_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:31,101 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:31,103 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220202_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   0%|          | 32/8299 [00:03<17:03,  8.08it/s]2025-04-26 18:50:31,143 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220201_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:31,144 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:31,147 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220201_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:31,172 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220205_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:31,212 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220203_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:31,213 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:31,218 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220203_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   0%|          | 34/8299 [00:03<14:31,  9.48it/s]2025-04-26 18:50:31,222 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220206_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:31,242 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220204_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:31,243 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:31,247 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220204_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:31,257 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220207_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:31,322 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220208_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:31,339 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220209_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:31,607 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220205_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:31,608 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:31,610 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220205_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   0%|          | 36/8299 [00:04<18:04,  7.62it/s]2025-04-26 18:50:31,696 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220207_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:31,697 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:31,700 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220207_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:31,714 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220210_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:31,731 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220208_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:31,732 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:31,736 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220208_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   0%|          | 38/8299 [00:04<15:19,  8.98it/s]2025-04-26 18:50:31,750 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220206_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:31,751 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:31,755 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220206_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:31,768 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220209_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:31,769 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:31,773 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220209_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:31,832 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220211_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:31,834 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220212_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:31,864 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220213_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:31,955 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220214_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:32,140 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220210_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:32,141 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:32,145 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220210_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   0%|          | 41/8299 [00:04<16:38,  8.27it/s]2025-04-26 18:50:32,227 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220215_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:32,235 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220211_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:32,236 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:32,239 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220211_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:32,271 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220213_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:32,272 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:32,275 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220213_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|          | 43/8299 [00:05<14:36,  9.42it/s]2025-04-26 18:50:32,280 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220212_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:32,281 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:32,284 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220212_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:32,349 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220216_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:32,353 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220214_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:32,354 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:32,357 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220214_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:32,378 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220217_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:32,384 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220218_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:32,461 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220219_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:32,612 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220215_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:32,613 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:32,616 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220215_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|          | 46/8299 [00:05<14:58,  9.19it/s]2025-04-26 18:50:32,752 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220220_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:32,762 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220216_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:32,763 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:32,766 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220216_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:32,839 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220218_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:32,840 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:32,843 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220218_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|          | 48/8299 [00:05<15:07,  9.09it/s]2025-04-26 18:50:32,853 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220221_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:32,861 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220217_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:32,862 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:32,865 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220217_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:32,980 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220222_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:33,015 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220223_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:33,056 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220219_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:33,057 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:33,060 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220219_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|          | 50/8299 [00:05<15:04,  9.12it/s]2025-04-26 18:50:33,163 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220224_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:33,247 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220220_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:33,248 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:33,251 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220220_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:33,289 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220221_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:33,290 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:33,293 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220221_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|          | 52/8299 [00:06<15:19,  8.97it/s]2025-04-26 18:50:33,348 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220225_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:33,396 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220222_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:33,397 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:33,400 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220222_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|          | 53/8299 [00:06<15:13,  9.03it/s]2025-04-26 18:50:33,460 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220223_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:33,461 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:33,464 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220223_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:33,509 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220226_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:33,534 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220224_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:33,536 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220227_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:33,537 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:33,543 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220224_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|          | 55/8299 [00:06<13:29, 10.19it/s]2025-04-26 18:50:33,593 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220228_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:33,639 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220301_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:33,771 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220225_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:33,772 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:33,774 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220225_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:33,883 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220302_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:33,986 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220227_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:33,987 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:33,989 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220227_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|          | 57/8299 [00:06<18:53,  7.27it/s]2025-04-26 18:50:34,003 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220226_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:34,004 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:34,006 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220226_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:34,062 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220228_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:34,063 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:34,066 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220228_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:34,121 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220304_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:34,122 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220303_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:34,208 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220305_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:34,359 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220302_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:34,360 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:34,363 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220302_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|          | 60/8299 [00:07<18:09,  7.56it/s]2025-04-26 18:50:34,486 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220301_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:34,488 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:34,491 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220301_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|          | 61/8299 [00:07<18:03,  7.60it/s]2025-04-26 18:50:34,507 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220303_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:34,508 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:34,510 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220303_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:34,610 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220307_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:34,641 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220305_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:34,642 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:34,644 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220305_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|          | 63/8299 [00:07<15:43,  8.72it/s]2025-04-26 18:50:34,655 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220308_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:34,665 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220306_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:34,706 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220304_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:34,708 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:34,710 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220304_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:34,767 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220309_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:34,830 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220310_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:35,078 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220307_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:35,079 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:35,083 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220307_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|          | 65/8299 [00:07<20:08,  6.81it/s]2025-04-26 18:50:35,094 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220306_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:35,095 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:35,098 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220306_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:35,165 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220309_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:35,166 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:35,169 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220309_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:35,199 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220308_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:35,201 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:35,203 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220312_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:35,204 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220308_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|          | 68/8299 [00:07<14:25,  9.51it/s]2025-04-26 18:50:35,248 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220311_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:35,264 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220313_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:35,320 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220314_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:35,397 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220310_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:35,398 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:35,401 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220310_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:35,511 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220315_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:35,619 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220312_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:35,620 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:35,624 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220312_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|          | 70/8299 [00:08<18:15,  7.51it/s]2025-04-26 18:50:35,709 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220311_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:35,711 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:35,713 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220311_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:35,747 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220314_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:35,748 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220316_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:35,749 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:35,755 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220314_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|          | 72/8299 [00:08<15:37,  8.77it/s]2025-04-26 18:50:35,798 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220317_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:35,853 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220313_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:35,854 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:35,856 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220313_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:35,881 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220318_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:35,933 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220315_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:35,934 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:35,938 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220315_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|          | 74/8299 [00:08<14:44,  9.30it/s]2025-04-26 18:50:35,967 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220319_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:36,070 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220320_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:36,238 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220317_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:36,239 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:36,242 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220317_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:36,264 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220316_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:36,266 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:36,269 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220316_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|          | 76/8299 [00:09<17:04,  8.03it/s]2025-04-26 18:50:36,319 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220318_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:36,320 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:36,323 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220318_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:36,326 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220321_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:36,379 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220322_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:36,422 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220319_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:36,424 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:36,427 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220319_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|          | 78/8299 [00:09<15:13,  9.00it/s]2025-04-26 18:50:36,459 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220320_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:36,460 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:36,463 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220320_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:36,465 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220323_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:36,533 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220324_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:36,576 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220325_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:36,815 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220321_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:36,816 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:36,819 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220321_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|          | 80/8299 [00:09<18:40,  7.34it/s]2025-04-26 18:50:36,834 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220322_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:36,835 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:36,838 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220322_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:36,925 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220323_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:36,926 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220326_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:36,927 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:36,935 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220323_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|          | 82/8299 [00:09<15:27,  8.86it/s]2025-04-26 18:50:36,948 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220324_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:36,949 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:36,953 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220324_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:36,956 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220325_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:36,957 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:36,960 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220325_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:36,993 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220327_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:37,056 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220328_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:37,069 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220330_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:37,080 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220329_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:37,321 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220326_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:37,322 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:37,325 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220326_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|          | 85/8299 [00:10<16:22,  8.36it/s]2025-04-26 18:50:37,368 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220327_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:37,369 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:37,371 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220327_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:37,429 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220331_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:37,479 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220328_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:37,481 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:37,483 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220401_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:37,485 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220328_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|          | 87/8299 [00:10<15:00,  9.12it/s]2025-04-26 18:50:37,491 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220330_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:37,494 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:37,496 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220330_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:37,503 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220329_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:37,504 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:37,507 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220329_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:37,613 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220402_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:37,624 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220403_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:37,628 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220404_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:37,811 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220331_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:37,812 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:37,815 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220331_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|          | 90/8299 [00:10<14:55,  9.17it/s]2025-04-26 18:50:37,913 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220405_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:37,972 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220402_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:37,973 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:37,978 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220402_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:37,980 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220401_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:37,981 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:37,984 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220401_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|          | 92/8299 [00:10<14:03,  9.73it/s]2025-04-26 18:50:38,056 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220404_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:38,057 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:38,060 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220404_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:38,075 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220406_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:38,185 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220408_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:38,233 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220403_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:38,234 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:38,237 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220403_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|          | 94/8299 [00:10<14:55,  9.16it/s]2025-04-26 18:50:38,275 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220407_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:38,307 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220405_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:38,308 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:38,311 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220405_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:38,357 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220409_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:38,406 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220410_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:38,418 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220406_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:38,419 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:38,423 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220406_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|          | 96/8299 [00:11<14:17,  9.57it/s]2025-04-26 18:50:38,528 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220411_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:38,621 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220408_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:38,622 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:38,625 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220408_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:38,685 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220407_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:38,686 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:38,689 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220407_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|          | 98/8299 [00:11<15:24,  8.87it/s]2025-04-26 18:50:38,753 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220409_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:38,754 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220410_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:38,755 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:38,756 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:38,760 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220409_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:38,764 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220410_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:38,786 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220413_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:38,837 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220412_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:38,850 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220411_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:38,851 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:38,855 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220411_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|          | 101/8299 [00:11<12:26, 10.98it/s]2025-04-26 18:50:38,856 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220415_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:38,877 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220414_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:38,968 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220416_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:39,218 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220413_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:39,219 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:39,222 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220413_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:39,236 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220412_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:39,237 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:39,240 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220412_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|          | 103/8299 [00:12<16:06,  8.48it/s]2025-04-26 18:50:39,284 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220414_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:39,286 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:39,288 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220414_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:39,301 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220415_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:39,303 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:39,306 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220415_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:39,344 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220417_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:39,363 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220418_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:39,404 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220416_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:39,405 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:39,409 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220416_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|▏         | 106/8299 [00:12<13:02, 10.47it/s]2025-04-26 18:50:39,424 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220420_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:39,428 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220419_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:39,527 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220421_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:39,747 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220419_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:39,748 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:39,751 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220419_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:39,758 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220417_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:39,759 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:39,760 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220417_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|▏         | 108/8299 [00:12<15:52,  8.60it/s]2025-04-26 18:50:39,784 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220418_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:39,785 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:39,787 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220418_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:39,845 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220422_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:39,874 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220423_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:39,884 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220421_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:39,885 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:39,887 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220421_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|▏         | 110/8299 [00:12<13:56,  9.79it/s]2025-04-26 18:50:39,891 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220424_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:39,903 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220420_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:39,904 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:39,907 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220420_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:40,002 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220426_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:40,025 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220425_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:40,267 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220423_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:40,268 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:40,272 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220423_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|▏         | 112/8299 [00:13<17:19,  7.88it/s]2025-04-26 18:50:40,305 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220424_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:40,306 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:40,310 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220424_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:40,319 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220422_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:40,320 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:40,323 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220422_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:40,416 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220427_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:40,422 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220428_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:40,427 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220426_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:40,428 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:40,431 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220426_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|▏         | 115/8299 [00:13<13:33, 10.06it/s]2025-04-26 18:50:40,443 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220425_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:40,444 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:40,446 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220425_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:40,465 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220429_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:40,541 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220501_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:40,550 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220430_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:40,844 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220429_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:40,845 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:40,849 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220429_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|▏         | 117/8299 [00:13<17:27,  7.81it/s]2025-04-26 18:50:40,859 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220428_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:40,860 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:40,863 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220428_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:40,901 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220430_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:40,902 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:40,905 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220430_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:40,936 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220501_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:40,938 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:40,940 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220501_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:40,955 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220502_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:40,966 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220427_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:40,968 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:40,970 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220427_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|▏         | 121/8299 [00:13<11:47, 11.56it/s]2025-04-26 18:50:40,974 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220503_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:41,025 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220504_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:41,064 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220505_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:41,101 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220506_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:41,356 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220505_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:41,357 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:41,360 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220505_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:41,367 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220502_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:41,368 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:41,371 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220502_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   1%|▏         | 123/8299 [00:14<15:22,  8.86it/s]2025-04-26 18:50:41,399 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220503_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:41,400 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:41,403 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220503_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:41,470 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220508_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:41,474 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220506_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:41,475 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:41,480 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220506_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 125/8299 [00:14<13:22, 10.19it/s]2025-04-26 18:50:41,497 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220507_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:41,501 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220509_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:41,525 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220504_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:41,526 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:41,528 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220504_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:41,600 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220510_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:41,658 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220511_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:41,852 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220509_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:41,853 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:41,857 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220509_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 127/8299 [00:14<16:37,  8.19it/s]2025-04-26 18:50:41,901 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220508_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:41,902 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:41,905 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220508_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:41,916 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220507_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:41,917 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:41,920 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220507_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:41,980 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220512_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:42,011 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220513_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:42,027 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220511_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:42,028 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:42,031 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220511_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 130/8299 [00:14<13:28, 10.11it/s]2025-04-26 18:50:42,041 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220514_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:42,063 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220510_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:42,064 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:42,067 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220510_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:42,134 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220515_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:42,189 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220516_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:42,386 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220513_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:42,387 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:42,390 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220513_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 132/8299 [00:15<16:17,  8.35it/s]2025-04-26 18:50:42,435 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220514_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:42,436 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:42,439 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220514_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:42,492 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220517_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:42,546 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220515_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:42,547 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220518_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:42,547 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:42,556 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220515_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 134/8299 [00:15<14:57,  9.10it/s]2025-04-26 18:50:42,583 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220516_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:42,584 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220512_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:42,584 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:42,586 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:42,589 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220516_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:42,593 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220512_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:42,666 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220519_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:42,688 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220520_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:42,718 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220521_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:42,803 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220517_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:42,804 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:42,807 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220517_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 137/8299 [00:15<13:38,  9.97it/s]2025-04-26 18:50:42,915 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220522_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:42,959 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220518_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:42,960 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:42,963 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220518_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:43,066 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220520_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:43,067 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:43,070 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220520_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 139/8299 [00:15<14:42,  9.24it/s]2025-04-26 18:50:43,093 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220523_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:43,120 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220519_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:43,121 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:43,125 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220519_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:43,127 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220521_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:43,128 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:43,130 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220521_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:43,161 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220524_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:43,234 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220526_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:43,254 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220525_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:43,455 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220524_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:43,456 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:43,461 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220524_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 142/8299 [00:16<15:46,  8.62it/s]2025-04-26 18:50:43,534 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220523_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:43,535 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:43,538 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220523_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:43,565 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220527_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:43,584 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220522_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:43,585 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:43,588 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220522_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 144/8299 [00:16<13:58,  9.73it/s]2025-04-26 18:50:43,596 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220525_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:43,597 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:43,600 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220525_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:43,603 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220526_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:43,604 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:43,608 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220526_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:43,667 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220528_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:43,687 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220529_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:43,736 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220531_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:43,749 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220530_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:43,953 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220527_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:43,954 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:43,957 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220527_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 147/8299 [00:16<14:55,  9.10it/s]2025-04-26 18:50:44,006 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220528_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:44,007 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:44,010 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220528_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:44,079 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220529_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:44,080 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:44,083 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220529_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 149/8299 [00:16<13:21, 10.17it/s]2025-04-26 18:50:44,097 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220602_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:44,108 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220601_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:44,123 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220531_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:44,124 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:44,127 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220531_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:44,188 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220603_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:44,245 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220604_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:44,430 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220530_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:44,431 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:44,434 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220530_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 151/8299 [00:17<16:05,  8.44it/s]2025-04-26 18:50:44,520 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220601_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:44,521 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:44,523 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220601_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:44,534 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220603_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:44,536 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:44,538 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220603_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 153/8299 [00:17<13:39,  9.94it/s]2025-04-26 18:50:44,543 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220605_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:44,611 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220602_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:44,612 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:44,615 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220604_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:44,616 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220602_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:44,619 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:44,624 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220604_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:44,646 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220606_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:44,652 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220607_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:44,748 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220609_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:44,756 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220608_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:44,935 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220605_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:44,936 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:44,939 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220605_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 156/8299 [00:17<15:16,  8.88it/s]2025-04-26 18:50:45,039 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220607_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:45,040 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:45,042 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220607_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:45,069 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220611_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:45,095 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220606_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:45,097 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:45,099 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220606_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 158/8299 [00:17<14:08,  9.59it/s]2025-04-26 18:50:45,137 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220612_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:45,201 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220613_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:45,269 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220609_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:45,270 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:45,274 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220609_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:45,295 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220608_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:45,296 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:45,299 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220608_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 160/8299 [00:18<13:57,  9.71it/s]2025-04-26 18:50:45,383 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220614_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:45,431 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220615_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:45,502 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220611_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:45,503 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:45,506 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220611_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:45,520 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220612_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:45,521 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:45,524 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220612_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 162/8299 [00:18<14:19,  9.47it/s]2025-04-26 18:50:45,535 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220613_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:45,536 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:45,539 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220613_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:45,623 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220616_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:45,630 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220617_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:45,639 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220618_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:45,769 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220614_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:45,770 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:45,772 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220614_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 164/8299 [00:18<15:01,  9.02it/s]2025-04-26 18:50:45,883 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220619_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:45,911 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220615_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:45,912 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:45,915 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220615_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 165/8299 [00:18<15:45,  8.60it/s]2025-04-26 18:50:45,952 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220617_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:45,953 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:45,956 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220617_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:45,973 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220618_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:45,974 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:45,977 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220618_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:46,024 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220616_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:46,025 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:46,029 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220616_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 168/8299 [00:18<11:17, 12.00it/s]2025-04-26 18:50:46,047 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220620_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:46,093 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220621_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:46,102 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220622_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:46,151 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220623_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:46,239 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220619_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:46,240 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:46,243 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220619_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:46,355 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220624_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:46,424 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220620_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:46,425 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:46,428 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220620_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 170/8299 [00:19<15:48,  8.57it/s]2025-04-26 18:50:46,455 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220621_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:46,456 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:46,458 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220621_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:46,513 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220622_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:46,514 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:46,517 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220622_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:46,535 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220623_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:46,536 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:46,538 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220623_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 173/8299 [00:19<11:41, 11.58it/s]2025-04-26 18:50:46,553 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220625_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:46,593 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220626_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:46,612 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220627_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:46,651 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220628_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:46,724 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220624_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:46,725 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:46,727 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220624_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:46,913 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220629_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:46,944 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220627_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:46,945 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:46,949 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220627_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 175/8299 [00:19<15:57,  8.48it/s]2025-04-26 18:50:46,995 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220625_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:46,996 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:46,998 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220625_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:47,005 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220626_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:47,006 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:47,008 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220626_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:47,032 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220630_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:47,074 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220628_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:47,075 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:47,079 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220628_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 178/8299 [00:19<12:17, 11.01it/s]2025-04-26 18:50:47,111 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220701_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:47,133 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220702_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:47,190 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220703_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:47,389 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220629_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:47,390 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:47,394 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220629_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:47,437 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220630_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:47,438 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:47,441 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220630_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 180/8299 [00:20<15:26,  8.76it/s]2025-04-26 18:50:47,445 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220702_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:47,447 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:47,449 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220702_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:47,498 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220703_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:47,499 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:47,501 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220703_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:47,505 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220704_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:47,548 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220705_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:47,561 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220706_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:47,608 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220707_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:47,725 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220701_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:47,726 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:47,729 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220701_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 183/8299 [00:20<14:33,  9.29it/s]2025-04-26 18:50:47,822 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220708_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:47,952 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220707_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:47,953 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:47,956 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220707_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:47,964 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220706_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:47,965 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:47,968 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220706_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 185/8299 [00:20<14:57,  9.04it/s]2025-04-26 18:50:47,971 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220705_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:47,973 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:47,976 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220705_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:47,977 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220704_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:47,979 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:47,983 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220704_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:48,055 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220711_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:48,060 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220709_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:48,106 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220712_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:48,123 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220710_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:48,282 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220708_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:48,283 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:48,286 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220708_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 188/8299 [00:21<14:43,  9.18it/s]2025-04-26 18:50:48,375 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220713_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:48,411 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220709_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:48,412 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:48,415 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220709_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:48,419 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220711_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:48,420 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:48,423 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220711_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 190/8299 [00:21<13:21, 10.12it/s]2025-04-26 18:50:48,459 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220712_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:48,460 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:48,462 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220712_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:48,490 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220714_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:48,536 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220715_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:48,537 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220710_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:48,540 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:48,544 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220710_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 192/8299 [00:21<11:58, 11.28it/s]2025-04-26 18:50:48,594 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220716_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:48,661 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220717_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:48,845 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220713_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:48,846 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:48,848 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220713_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:48,949 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220718_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:48,991 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220715_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:48,992 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:48,995 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220715_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 194/8299 [00:21<17:03,  7.92it/s]2025-04-26 18:50:49,037 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220716_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:49,038 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:49,042 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220716_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:49,079 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220714_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:49,080 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:49,084 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220714_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:49,121 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220719_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:49,184 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220720_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:49,188 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220717_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:49,190 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:49,192 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220717_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 197/8299 [00:21<14:02,  9.62it/s]2025-04-26 18:50:49,197 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220721_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:49,289 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220722_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:49,354 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220718_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:49,355 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:49,358 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220718_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:49,493 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220723_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:49,518 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220720_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:49,520 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:49,523 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220720_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 199/8299 [00:22<16:09,  8.35it/s]2025-04-26 18:50:49,552 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220719_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:49,553 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:49,556 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220719_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:49,622 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220724_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:49,685 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220725_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:49,705 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220722_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:49,706 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:49,709 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220722_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 201/8299 [00:22<15:12,  8.88it/s]2025-04-26 18:50:49,788 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220726_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:49,807 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220723_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:49,808 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:49,811 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220723_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:49,896 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220727_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:50,053 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220721_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:50,055 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:50,058 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220721_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 203/8299 [00:22<17:30,  7.71it/s]2025-04-26 18:50:50,072 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220724_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:50,073 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:50,077 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220724_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:50,102 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220725_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:50,103 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:50,106 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220725_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:50,185 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220729_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:50,191 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220726_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:50,193 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:50,196 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220726_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   2%|▏         | 206/8299 [00:22<13:18, 10.14it/s]2025-04-26 18:50:50,198 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220728_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:50,228 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220730_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:50,266 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220727_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:50,267 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:50,270 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220727_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:50,305 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220731_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:50,358 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220801_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:50,553 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220728_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:50,555 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:50,558 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220728_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   3%|▎         | 208/8299 [00:23<16:11,  8.33it/s]2025-04-26 18:50:50,593 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220729_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:50,594 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:50,597 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220729_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:50,668 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220731_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:50,669 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:50,672 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220731_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   3%|▎         | 210/8299 [00:23<13:52,  9.72it/s]2025-04-26 18:50:50,684 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220802_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:50,708 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220801_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:50,709 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:50,713 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220801_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:50,733 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220803_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:50,749 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220804_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:50,753 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220730_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:50,754 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:50,757 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220730_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:50,831 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220805_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:50,849 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220806_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:51,026 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220803_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:51,027 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:51,030 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220803_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   3%|▎         | 213/8299 [00:23<14:40,  9.18it/s]2025-04-26 18:50:51,063 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220802_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:51,064 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:51,068 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220802_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:51,113 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220807_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:51,208 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220808_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:51,216 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220806_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:51,217 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:51,220 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220806_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   3%|▎         | 215/8299 [00:23<14:10,  9.51it/s]2025-04-26 18:50:51,228 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220805_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:51,229 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:51,233 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220805_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:51,323 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220810_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:51,328 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220804_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:51,329 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:51,332 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220804_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   3%|▎         | 217/8299 [00:24<12:22, 10.88it/s]2025-04-26 18:50:51,336 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220809_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:51,426 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220807_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:51,427 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:51,429 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220807_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:51,526 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220812_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:51,601 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220808_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:51,602 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:51,605 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220808_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   3%|▎         | 219/8299 [00:24<14:03,  9.58it/s]2025-04-26 18:50:51,629 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220809_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:51,630 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:51,633 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220809_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:51,639 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220811_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:51,725 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220810_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:51,726 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:51,730 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220810_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   3%|▎         | 221/8299 [00:24<12:25, 10.83it/s]2025-04-26 18:50:51,734 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220813_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:51,789 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220814_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:51,827 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220815_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:52,007 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220811_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:52,008 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:52,012 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220811_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:52,074 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220812_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:52,075 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:52,078 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220812_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   3%|▎         | 223/8299 [00:24<15:37,  8.62it/s]2025-04-26 18:50:52,101 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220816_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:52,154 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220814_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:52,156 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:52,159 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220814_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:52,171 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220813_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:52,172 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:52,175 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220813_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:52,178 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220817_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:52,248 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220818_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:52,256 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220815_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:52,258 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:52,260 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220815_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   3%|▎         | 226/8299 [00:25<12:45, 10.54it/s]2025-04-26 18:50:52,347 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220820_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:52,351 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220819_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:52,634 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220817_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:52,635 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:52,636 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220817_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:52,686 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220820_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:52,688 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:52,690 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220820_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   3%|▎         | 228/8299 [00:25<17:06,  7.87it/s]2025-04-26 18:50:52,734 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220818_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:52,735 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:52,738 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220818_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:52,747 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220821_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:52,756 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220819_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:52,757 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:52,760 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220819_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:52,834 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220822_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:52,846 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220824_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:52,941 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220823_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:53,121 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220821_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:53,122 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:53,124 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220821_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   3%|▎         | 231/8299 [00:25<17:56,  7.49it/s]2025-04-26 18:50:53,158 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220816_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:53,159 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:53,162 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220816_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:53,264 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220825_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:53,283 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220826_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:53,375 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220824_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:53,377 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:53,376 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220822_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:53,381 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220824_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   3%|▎         | 233/8299 [00:26<17:48,  7.55it/s]2025-04-26 18:50:53,383 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:53,384 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220823_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:53,389 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220822_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:53,390 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:53,395 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220823_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:53,492 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220827_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:53,493 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220829_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:53,499 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220828_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:53,644 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220826_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:53,645 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:53,648 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220826_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   3%|▎         | 236/8299 [00:26<15:40,  8.57it/s]2025-04-26 18:50:53,764 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220830_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:53,852 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220828_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:53,853 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:53,856 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220828_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   3%|▎         | 237/8299 [00:26<17:27,  7.70it/s]2025-04-26 18:50:53,909 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220825_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:53,910 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:53,913 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220825_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:53,916 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220827_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:53,917 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:53,920 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220827_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:53,960 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220829_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:53,961 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:53,964 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220829_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   3%|▎         | 240/8299 [00:26<12:36, 10.65it/s]2025-04-26 18:50:53,997 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220831_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:54,018 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220901_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:54,024 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220902_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:54,116 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220903_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:54,203 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220830_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:54,204 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:54,207 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220830_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:54,321 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220904_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:54,440 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220831_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:54,441 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:54,443 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220831_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   3%|▎         | 242/8299 [00:27<17:51,  7.52it/s]2025-04-26 18:50:54,469 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220901_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:54,471 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:54,474 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220901_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:54,541 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220905_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:54,546 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220903_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:54,547 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:54,550 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220903_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   3%|▎         | 244/8299 [00:27<14:53,  9.01it/s]2025-04-26 18:50:54,560 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220902_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:54,561 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:54,564 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220902_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:54,599 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220906_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:54,649 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220907_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:54,674 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220908_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:54,675 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220904_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:54,678 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:54,682 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220904_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   3%|▎         | 246/8299 [00:27<13:10, 10.19it/s]2025-04-26 18:50:54,765 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220909_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:54,864 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220905_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:54,865 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:54,869 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220905_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:54,981 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220910_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:55,003 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220906_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:55,005 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:55,008 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220906_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   3%|▎         | 248/8299 [00:27<15:39,  8.57it/s]2025-04-26 18:50:55,042 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220908_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:55,044 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:55,047 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220908_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:55,064 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220907_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:55,066 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:55,069 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220907_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:55,103 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220911_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:55,136 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220909_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:55,137 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:55,141 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220909_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   3%|▎         | 251/8299 [00:27<11:57, 11.22it/s]2025-04-26 18:50:55,160 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220912_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:55,212 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220913_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:55,248 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220914_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:55,406 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220910_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:55,407 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:55,410 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220910_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:55,478 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220912_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:55,479 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:55,482 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220912_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   3%|▎         | 253/8299 [00:28<14:52,  9.02it/s]2025-04-26 18:50:55,548 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220915_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:55,555 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220913_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:55,556 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:55,560 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220913_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:55,562 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220916_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:55,566 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220914_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:55,568 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:55,572 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220914_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:55,574 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220911_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:55,575 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:55,577 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220911_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:55,653 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220918_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:55,674 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220920_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:55,690 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220919_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:55,962 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220915_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:55,963 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:55,966 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220915_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   3%|▎         | 257/8299 [00:28<15:27,  8.67it/s]2025-04-26 18:50:56,009 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220918_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:56,010 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:56,014 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220918_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:56,015 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220916_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:56,018 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:56,021 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220916_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:56,026 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220920_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:56,028 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:56,031 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220920_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:56,061 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220919_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:56,063 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:56,066 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220919_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:56,144 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220923_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:56,149 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220922_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:56,152 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220924_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:56,160 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220926_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:56,223 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220921_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:56,504 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220924_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:56,505 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:56,507 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220924_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   3%|▎         | 262/8299 [00:29<15:01,  8.92it/s]2025-04-26 18:50:56,532 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220926_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:56,533 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:56,537 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220926_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:56,588 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220923_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:56,589 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:56,591 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220923_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:56,598 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220927_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:56,617 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220928_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:56,648 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220921_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:56,649 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:56,652 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220921_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   3%|▎         | 265/8299 [00:29<12:41, 10.55it/s]2025-04-26 18:50:56,669 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220929_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:56,718 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220922_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:56,719 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:56,721 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220922_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:56,771 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20220930_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:56,842 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221001_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:57,026 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220928_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:57,027 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:57,031 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220928_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   3%|▎         | 267/8299 [00:29<15:17,  8.75it/s]2025-04-26 18:50:57,083 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220927_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:57,084 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:57,087 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220927_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:57,095 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220929_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:57,096 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:57,100 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220929_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:57,128 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221002_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:57,196 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221001_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:57,198 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:57,203 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221001_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   3%|▎         | 270/8299 [00:29<12:59, 10.30it/s]2025-04-26 18:50:57,205 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221005_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:57,205 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221003_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:57,256 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20220930_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:57,257 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:57,260 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20220930_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:57,311 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221007_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:57,375 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221008_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:57,506 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221002_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:57,507 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:57,510 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221002_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   3%|▎         | 272/8299 [00:30<14:40,  9.12it/s]2025-04-26 18:50:57,528 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221005_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:57,529 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:57,532 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221005_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:57,549 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221003_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:57,551 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:57,554 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221003_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:57,623 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221009_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:57,630 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221010_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:57,685 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221007_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:57,686 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:57,688 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221007_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   3%|▎         | 275/8299 [00:30<12:27, 10.73it/s]2025-04-26 18:50:57,699 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221011_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:57,704 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221008_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:57,705 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:57,709 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221008_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:57,771 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221012_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:57,835 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221014_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:57,970 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221010_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:57,971 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:57,974 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221010_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   3%|▎         | 277/8299 [00:30<14:02,  9.52it/s]2025-04-26 18:50:57,996 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221009_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:57,997 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:57,999 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221009_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:58,057 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221012_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:58,058 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:58,060 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221012_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:58,069 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221011_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:58,071 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:58,072 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221015_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:58,074 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221011_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   3%|▎         | 280/8299 [00:30<10:52, 12.29it/s]2025-04-26 18:50:58,103 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221016_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:58,132 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221017_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:58,167 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221018_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:58,176 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221014_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:58,178 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:58,183 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221014_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:58,259 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221019_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:58,426 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221016_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:58,427 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:58,430 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221016_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   3%|▎         | 282/8299 [00:31<13:56,  9.59it/s]2025-04-26 18:50:58,486 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221015_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:58,487 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:58,491 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221015_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:58,508 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221020_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:58,526 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221017_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:58,527 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:58,530 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221017_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:58,605 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221022_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:58,620 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221023_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:58,638 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221018_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:58,639 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:58,642 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221018_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   3%|▎         | 285/8299 [00:31<12:22, 10.79it/s]2025-04-26 18:50:58,708 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221019_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:58,710 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:58,713 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221019_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:58,764 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221024_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:58,820 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221025_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:58,919 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221022_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:58,920 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:58,922 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221022_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   3%|▎         | 287/8299 [00:31<13:56,  9.57it/s]2025-04-26 18:50:58,930 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221023_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:58,931 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:58,934 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221023_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:59,028 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221027_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:59,037 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221026_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:59,045 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221020_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:59,046 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:59,049 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221020_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   3%|▎         | 289/8299 [00:31<12:30, 10.67it/s]2025-04-26 18:50:59,132 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221025_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:59,133 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:59,135 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221025_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:59,176 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221028_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:59,269 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221029_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:59,317 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221024_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:59,318 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:59,321 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221024_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   4%|▎         | 291/8299 [00:32<14:02,  9.51it/s]2025-04-26 18:50:59,373 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221027_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:59,375 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:59,378 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221027_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:59,437 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221026_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:59,438 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:59,441 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221026_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   4%|▎         | 293/8299 [00:32<12:21, 10.80it/s]2025-04-26 18:50:59,448 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221030_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:59,487 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221031_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:59,539 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221101_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:59,587 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221028_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:59,588 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:59,593 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221028_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:59,654 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221029_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:59,655 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:59,656 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221029_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   4%|▎         | 295/8299 [00:32<12:56, 10.30it/s]2025-04-26 18:50:59,680 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221102_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:59,752 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221103_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:59,778 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221030_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:59,779 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:59,783 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221030_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:59,868 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221104_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:59,881 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221101_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:59,882 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:59,885 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221101_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   4%|▎         | 297/8299 [00:32<13:35,  9.81it/s]2025-04-26 18:50:59,957 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221106_cleaned. Funnel mapping might fail.
    2025-04-26 18:50:59,988 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221031_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:59,989 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:50:59,992 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221031_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:00,052 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221102_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:00,053 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:00,057 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221102_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   4%|▎         | 299/8299 [00:32<12:56, 10.30it/s]2025-04-26 18:51:00,107 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221107_cleaned. Funnel mapping might fail.
    2025-04-26 18:51:00,139 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221103_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:00,140 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:00,143 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221103_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:00,161 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221108_cleaned. Funnel mapping might fail.
    2025-04-26 18:51:00,220 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221104_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:00,221 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:00,224 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221104_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   4%|▎         | 301/8299 [00:32<12:26, 10.72it/s]2025-04-26 18:51:00,292 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221109_cleaned. Funnel mapping might fail.
    2025-04-26 18:51:00,307 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221106_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:00,308 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:00,312 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221106_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:00,331 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221110_cleaned. Funnel mapping might fail.
    2025-04-26 18:51:00,410 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221111_cleaned. Funnel mapping might fail.
    2025-04-26 18:51:00,419 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221107_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:00,420 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:00,423 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221107_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   4%|▎         | 303/8299 [00:33<12:39, 10.52it/s]2025-04-26 18:51:00,549 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221108_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:00,550 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:00,553 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221108_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:00,576 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221112_cleaned. Funnel mapping might fail.
    2025-04-26 18:51:00,633 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221109_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:00,634 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:00,638 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221109_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   4%|▎         | 305/8299 [00:33<13:09, 10.12it/s]2025-04-26 18:51:00,659 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221113_cleaned. Funnel mapping might fail.
    2025-04-26 18:51:00,741 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221114_cleaned. Funnel mapping might fail.
    2025-04-26 18:51:00,763 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221110_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:00,764 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:00,766 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221110_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:00,813 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221111_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:00,814 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:00,817 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221111_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   4%|▎         | 307/8299 [00:33<12:47, 10.42it/s]2025-04-26 18:51:00,866 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221115_cleaned. Funnel mapping might fail.
    2025-04-26 18:51:00,909 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221116_cleaned. Funnel mapping might fail.
    2025-04-26 18:51:00,929 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221112_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:00,930 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:00,932 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221112_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:01,049 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221117_cleaned. Funnel mapping might fail.
    2025-04-26 18:51:01,057 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221113_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:01,058 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:01,061 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221113_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   4%|▎         | 309/8299 [00:33<13:48,  9.64it/s]2025-04-26 18:51:01,173 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221118_cleaned. Funnel mapping might fail.
    2025-04-26 18:51:01,177 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221114_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:01,178 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:01,180 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221114_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   4%|▎         | 310/8299 [00:33<14:12,  9.37it/s]2025-04-26 18:51:01,265 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221116_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:01,266 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:01,269 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221116_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:01,286 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221119_cleaned. Funnel mapping might fail.
    2025-04-26 18:51:01,312 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221115_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:01,313 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:01,315 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221115_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   4%|▍         | 312/8299 [00:34<12:26, 10.71it/s]2025-04-26 18:51:01,360 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221120_cleaned. Funnel mapping might fail.
    2025-04-26 18:51:01,423 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221121_cleaned. Funnel mapping might fail.
    2025-04-26 18:51:01,439 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221117_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:01,440 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:01,443 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221117_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:01,551 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221123_cleaned. Funnel mapping might fail.
    2025-04-26 18:51:01,567 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221118_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:01,568 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:01,571 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221118_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   4%|▍         | 314/8299 [00:34<13:55,  9.56it/s]2025-04-26 18:51:01,689 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221124_cleaned. Funnel mapping might fail.
    2025-04-26 18:51:01,924 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221120_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:01,925 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:01,929 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221120_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:01,932 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221123_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:01,933 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:01,937 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221123_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   4%|▍         | 316/8299 [00:34<17:12,  7.73it/s]2025-04-26 18:51:01,994 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221121_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:01,995 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:01,999 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221121_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:02,065 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221126_cleaned. Funnel mapping might fail.
    2025-04-26 18:51:02,071 - ERROR - 2698362772:399 - Error processing table mycocoons_Shopify_SHP_SALES_20221119_cleaned for KG: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:02,073 - ERROR - 2698362772:200 - Function process_table_for_kg encountered non-retryable error: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    2025-04-26 18:51:02,074 - ERROR - 2698362772:870 - Failed to process table mycocoons_Shopify_SHP_SALES_20221119_cleaned: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Traceback (most recent call last):
      File "/var/tmp/ipykernel_4258/2698362772.py", line 859, in main
        df = future.result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 451, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
        raise self._exception
      File "/opt/conda/lib/python3.10/concurrent/futures/thread.py", line 58, in run
        result = self.fn(*self.args, **self.kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 182, in wrapper
        return func(*args, **kwargs)
      File "/var/tmp/ipykernel_4258/2698362772.py", line 365, in process_table_for_kg
        df = query_job.to_dataframe(timeout=600) # 10 min timeout
    TypeError: QueryJob.to_dataframe() got an unexpected keyword argument 'timeout'
    Reading BQ Tables:   4%|▍         | 318/8299 [00:34<14:42,  9.04it/s]2025-04-26 18:51:02,083 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221125_cleaned. Funnel mapping might fail.
    2025-04-26 18:51:02,098 - WARNING - 2698362772:312 - Required 'event' or 'event_name' column not found in table mycocoons_Shopify_SHP_SALES_20221127_cleaned. Funnel mapping might fail.



```python

```
