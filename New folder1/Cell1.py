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
