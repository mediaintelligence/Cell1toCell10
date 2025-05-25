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
# Input table pattern (assuming Cell 3 creates these)
CLEAN_TABLE_PREFIX = f"clean_mycocoons_"

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

# --- SQL Generation ---

def generate_journey_sql(project: str, dataset: str, target_date_suffix: str, target_table_id: str) -> str:
    """Generates the BigQuery SQL to build the customer journey table."""

    # NOTE: This SQL needs careful refinement based on the *actual* columns
    # available and standardized in your `clean_*` tables after Cell 3 runs.
    # The COALESCE logic, event_name selection, and event_properties STRUCT
    # are examples and must be adapted to your specific schemas.

    sql = f"""
    CREATE OR REPLACE TABLE `{target_table_id}`
    PARTITION BY DATE(event_timestamp)
    CLUSTER BY customer_id, source_system, event_name
    OPTIONS (
      description="Unified, chronologically ordered customer events as of {datetime.utcnow().isoformat()}Z"
    ) AS
    WITH SourceEvents AS (
      -- ====================================
      -- Facebook Ads Events
      -- ====================================
      SELECT
        -- ** Customer ID Logic (Adapt!) **
        -- Assuming 'ad_campaign_id' or similar might contain user info indirectly?
        -- This needs a reliable way to link FB ad interactions to a customer ID.
        -- Using placeholder logic here. Needs refinement.
        'fb_' || COALESCE(CAST(ad_id AS STRING), 'unknown') AS customer_id, -- Example: Using ad_id as proxy ID? Unlikely correct.

        -- ** Event Timestamp Logic (Adapt!) **
        -- Assuming reporting_starts or date_start represents the event time
        COALESCE(CAST(reporting_starts AS TIMESTAMP), CAST(date_start AS TIMESTAMP)) AS event_timestamp,

        -- ** Event Name Logic (Adapt!) **
        -- Creating generic event names based on available metrics
        CASE
            WHEN clicks > 0 THEN 'facebook_ad_click'
            WHEN impressions > 0 THEN 'facebook_ad_impression'
            ELSE 'facebook_ad_other'
        END AS event_name,

        -- ** Source System **
        'Facebook' AS source_system,
        _TABLE_SUFFIX AS source_table_suffix, -- e.g., Facebook_FBADS_AD_20220101

        -- ** Event Properties (Adapt!) **
        TO_JSON_STRING(STRUCT(
            campaign_id,
            ad_id,
            geo,
            spend,
            clicks,
            impressions,
            reach,
            frequency
            -- Add other relevant Facebook columns
        )) AS event_properties
      FROM `{project}.{dataset}.clean_mycocoons_Facebook_*`
      -- Filter for the target processing date suffix and ensure timestamp exists
      WHERE _TABLE_SUFFIX LIKE '%_{target_date_suffix}'
        AND COALESCE(CAST(reporting_starts AS TIMESTAMP), CAST(date_start AS TIMESTAMP)) IS NOT NULL

      UNION ALL

      -- ====================================
      -- GA4 Custom Events
      -- ====================================
      SELECT
        -- ** Customer ID Logic (Adapt!) **
        COALESCE(CAST(user_pseudo_id AS STRING), unified_id, 'unknown_ga4') AS customer_id,

        -- ** Event Timestamp Logic (Adapt!) **
        CAST(event_datetime AS TIMESTAMP) AS event_timestamp, -- From Cell 3 cleaning

        -- ** Event Name Logic (Adapt!) **
        CAST(event_name AS STRING) AS event_name,

        -- ** Source System **
        'GA4Custom' AS source_system,
        _TABLE_SUFFIX AS source_table_suffix,

        -- ** Event Properties (Adapt!) **
        TO_JSON_STRING(STRUCT(
            ga_session_id,
            stream_id,
            engagement_time_msec -- Add other relevant GA4 columns/params if flattened
            -- Include flattened params like param_page_location, param_term etc. if available
        )) AS event_properties
      FROM `{project}.{dataset}.clean_mycocoons_GA4Custom_*`
      WHERE _TABLE_SUFFIX LIKE '%_{target_date_suffix}'
        AND event_datetime IS NOT NULL

      UNION ALL

      -- ====================================
      -- GA Analytics (UA/GA3) Events
      -- ====================================
      SELECT
        -- ** Customer ID Logic (Adapt!) **
        COALESCE(CAST(ga_clientid AS STRING), CAST(ga_userid AS STRING), unified_id, 'unknown_ga3') AS customer_id,

        -- ** Event Timestamp Logic (Adapt!) **
        -- Combine date and potentially time? GA3 often lacks precise hit timestamps in aggregate reports
        -- Using date as the primary timestamp here, might need adjustment
        CAST(COALESCE(session_date, date) AS TIMESTAMP) AS event_timestamp,

        -- ** Event Name Logic (Adapt!) **
        -- Infer event from table type or specific columns if possible
        CASE
            WHEN REGEXP_CONTAINS(_TABLE_SUFFIX, 'ECOM_ITEMS') THEN 'ga3_ecommerce_item'
            WHEN REGEXP_CONTAINS(_TABLE_SUFFIX, 'TRAFFIC') THEN 'ga3_session_start' -- Or derive from landing page?
            WHEN pageviews > 0 THEN 'ga3_pageview'
            ELSE 'ga3_generic_event'
        END AS event_name,

        -- ** Source System **
        'GAAnalytics' AS source_system,
        _TABLE_SUFFIX AS source_table_suffix,

        -- ** Event Properties (Adapt!) **
        TO_JSON_STRING(STRUCT(
            ga_sessionid,
            source,
            medium,
            campaign,
            page_path,
            hostname,
            transaction_id,
            item_revenue,
            item_quantity
            -- Add other relevant GA3 columns
        )) AS event_properties
      FROM `{project}.{dataset}.clean_mycocoons_GAAnalytics_*`
      WHERE _TABLE_SUFFIX LIKE '%_{target_date_suffix}'
        AND COALESCE(session_date, date) IS NOT NULL

      UNION ALL

      -- ====================================
      -- Google Ads Events
      -- ====================================
       SELECT
        -- ** Customer ID Logic (Adapt!) **
        -- Need a way to link Ad interactions (clicks) to customer ID (e.g., via GCLID joined later?)
        -- Using campaign_id as placeholder ID - NEEDS REFINEMENT
         'gads_' || COALESCE(CAST(campaign_id AS STRING), 'unknown') AS customer_id,

        -- ** Event Timestamp Logic (Adapt!) **
        CAST(date AS TIMESTAMP) AS event_timestamp,

        -- ** Event Name Logic (Adapt!) **
        CASE
            WHEN clicks > 0 THEN 'google_ad_click'
            WHEN impressions > 0 THEN 'google_ad_impression'
            -- WHEN conversions > 0 THEN 'google_ad_conversion' -- Conversion might be linked elsewhere
            ELSE 'google_ad_other'
        END AS event_name,

        -- ** Source System **
        'GoogleAds' AS source_system,
        _TABLE_SUFFIX AS source_table_suffix,

        -- ** Event Properties (Adapt!) **
        TO_JSON_STRING(STRUCT(
            campaign_id,
            ad_group_id,
            ad_id,
            keyword_id, -- Might need text if available
            cost,
            clicks,
            impressions,
            conversions,
            ctr,
            cpc
            -- Add other relevant Google Ads columns
        )) AS event_properties
      FROM `{project}.{dataset}.clean_mycocoons_Google_*`
      WHERE _TABLE_SUFFIX LIKE '%_{target_date_suffix}'
        AND date IS NOT NULL

      UNION ALL

      -- ====================================
      -- Shopify Events
      -- ====================================
      SELECT
        -- ** Customer ID Logic (Adapt!) **
        COALESCE(CAST(customer_id AS STRING), 'unknown_shp') AS customer_id,

        -- ** Event Timestamp Logic (Adapt!) **
        -- Use created_at for most events, potentially updated_at for status changes?
        CAST(created_at AS TIMESTAMP) AS event_timestamp,

        -- ** Event Name Logic (Adapt!) **
        -- Infer event from status or table type (assuming different exports exist)
        -- This is highly dependent on which Shopify exports are used
        CASE
            WHEN REGEXP_CONTAINS(_TABLE_SUFFIX, 'ORDER') THEN 'shopify_order_created' -- Crude example
            WHEN REGEXP_CONTAINS(_TABLE_SUFFIX, 'CHECKOUT') THEN 'shopify_checkout_created'
            ELSE 'shopify_generic_event'
        END AS event_name,
        -- Alternative: Use financial_status or fulfillment_status if more indicative

        -- ** Source System **
        'Shopify' AS source_system,
        _TABLE_SUFFIX AS source_table_suffix,

        -- ** Event Properties (Adapt!) **
        TO_JSON_STRING(STRUCT(
            order_id,
            checkout_id,
            cart_token,
            total_price,
            subtotal_price,
            total_discounts,
            financial_status,
            fulfillment_status
            -- Add other relevant Shopify columns
        )) AS event_properties
      FROM `{project}.{dataset}.clean_mycocoons_Shopify_*`
      WHERE _TABLE_SUFFIX LIKE '%_{target_date_suffix}'
        AND created_at IS NOT NULL

      -- ====================================
      -- Add UNION ALL for other sources here
      -- ====================================

    ),
    SequencedEvents AS (
      SELECT
        *,
        -- Generate a unique event ID (hash of key fields + timestamp)
        -- Using FARM_FINGERPRINT for better distribution than simple hashing
        CAST(FARM_FINGERPRINT(
            customer_id || FORMAT_TIMESTAMP('%Y%m%d%H%M%S%f', event_timestamp) || event_name || source_system || source_table_suffix
        ) AS STRING) AS event_id,

        -- Assign sequence number per customer
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY event_timestamp ASC, source_system ASC, source_table_suffix ASC -- Deterministic order
        ) AS journey_step_seq

        -- ** Optional: Sessionization Logic **
        /*
        , LAG(event_timestamp) OVER (PARTITION BY customer_id ORDER BY event_timestamp ASC) as prev_event_timestamp
        , TIMESTAMP_DIFF(event_timestamp, LAG(event_timestamp) OVER (PARTITION BY customer_id ORDER BY event_timestamp ASC), MINUTE) as minutes_since_last_event
        , SUM(CASE WHEN TIMESTAMP_DIFF(event_timestamp, LAG(event_timestamp) OVER (PARTITION BY customer_id ORDER BY event_timestamp ASC), MINUTE) > 30 OR LAG(event_timestamp) OVER (PARTITION BY customer_id ORDER BY event_timestamp ASC) IS NULL THEN 1 ELSE 0 END)
             OVER (PARTITION BY customer_id ORDER BY event_timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as session_increment
        , FARM_FINGERPRINT(customer_id || CAST(SUM(CASE WHEN TIMESTAMP_DIFF(event_timestamp, LAG(event_timestamp) OVER (PARTITION BY customer_id ORDER BY event_timestamp ASC), MINUTE) > 30 OR LAG(event_timestamp) OVER (PARTITION BY customer_id ORDER BY event_timestamp ASC) IS NULL THEN 1 ELSE 0 END)
             OVER (PARTITION BY customer_id ORDER BY event_timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS STRING)) as session_id_fingerprint
        */

      FROM SourceEvents
      WHERE customer_id IS NOT NULL AND customer_id != 'unknown' AND customer_id != '' -- Filter out events without a valid customer ID
        AND event_timestamp IS NOT NULL -- Ensure timestamp is valid
    )
    -- Final selection of columns for the journey table
    SELECT
      customer_id,
      event_timestamp,
      event_id,
      event_name,
      source_system,
      source_table_suffix, -- Keep for debugging/lineage
      event_properties,
      journey_step_seq
      -- CAST(session_id_fingerprint AS STRING) as session_id -- If sessionization enabled
    FROM SequencedEvents;
    """
    # Use textwrap.dedent to handle indentation in the multi-line SQL string
    return textwrap.dedent(sql)


@retry_with_backoff
def run_bq_job(sql: str):
    """Runs a BigQuery SQL job."""
    if not bq_client: raise RuntimeError("BigQuery client not initialized.")
    logger.info("Starting BigQuery job to build customer journey table...")
    logger.debug(f"Executing SQL:\n{sql[:1000]}...") # Log truncated SQL

    job_config = bigquery.QueryJobConfig(
        priority=bigquery.QueryPriority.BATCH # Use batch for potentially long-running job
    )

    query_job = bq_client.query(sql, job_config=job_config, location=LOCATION)
    logger.info(f"BigQuery job started. Job ID: {query_job.job_id}")

    # Wait for job completion (adjust timeout as needed for potentially large jobs)
    query_job.result(timeout=3600) # Wait up to 1 hour

    if query_job.errors:
        logger.error(f"BigQuery job {query_job.job_id} failed: {query_job.errors}")
        raise GoogleAPIError(f"BigQuery job failed: {query_job.errors}")
    else:
        logger.info(f"BigQuery job {query_job.job_id} completed successfully.")
        # Log details about the created/replaced table
        dest_table = bq_client.get_table(JOURNEY_TABLE_ID)
        logger.info(f"Table `{JOURNEY_TABLE_ID}` updated. Rows: {dest_table.num_rows}, Size: {dest_table.num_bytes / (1024**2):.2f} MB")

from tenacity import retry, stop_after_attempt, wait_fixed

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def run_bq_job(sql: str):
    logger.info("Running BigQuery job...")
    try:
        job = bq_client.query(sql)
        logger.info(f"Job started: {job.job_id}")
        job.result(timeout=1800)
        logger.info("Job complete!")
    except Exception as ex:
        logger.exception("BigQuery job failed")
        raise ex

target_date_suffix = "20250425"    # Adjust as needed for your workflow
journey_table = "cust_journey_events"
sql = generate_journey_sql_dyn(PROJECT_ID, DATASET_ID, target_date_suffix, journey_table)
run_bq_job(sql)



def main():
    """Main function to build the customer journey table."""
    logger.info(f"Starting Customer Journey Builder for date suffix: {TARGET_DATE_SUFFIX}...")
    start_time = time.time()

    try:
        initialize_clients()

        # Generate the dynamic SQL
        sql = generate_journey_sql(
            project=PROJECT_ID,
            dataset=DATASET_ID,
            target_date_suffix=TARGET_DATE_SUFFIX,
            target_table_id=JOURNEY_TABLE_ID
        )

        # Run the BigQuery job
        run_bq_job(sql)

        end_time = time.time()
        logger.info(f"Customer Journey Builder finished successfully in {end_time - start_time:.2f} seconds.")

        # **Next Step:** This job completion could trigger Cell 5 (Funnel/KG)
        #                e.g., by publishing a message to another Pub/Sub topic.
        # trigger_kg_build_job(JOURNEY_TABLE_ID, TARGET_DATE_SUFFIX) # Placeholder

    except Exception as e:
        logger.critical(f"Customer Journey Builder failed: {e}", exc_info=True)
        # Add alerting or monitoring hooks here

if __name__ == "__main__":
    main()
