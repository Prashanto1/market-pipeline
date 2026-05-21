import json
import logging
import os
import time

from db import init_db, upsert_records
from pipeline import calculate_vwap, enrich_records, fetch_data, validate_records

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("etl.main")

# ── Config from environment ───────────────────────────────────────────────────
API_URL       = os.getenv("API_URL", "http://api:8000/v1/market-data")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", 5))
MAX_RETRIES   = int(os.getenv("MAX_RETRIES", 3))
DB_READY_WAIT = int(os.getenv("DB_READY_WAIT_SECONDS", 5))


# ── Single pipeline run ───────────────────────────────────────────────────────

def run_pipeline():
    start = time.time()
    logger.info("── Pipeline run starting ──")

    # EXTRACT with retry + exponential backoff
    raw_records = []
    for attempt in range(1, MAX_RETRIES + 1):
        raw_records, error = fetch_data(API_URL)
        if raw_records:
            logger.info(f"Fetched {len(raw_records)} raw records (attempt {attempt})")
            break
        logger.warning(f"Fetch attempt {attempt}/{MAX_RETRIES} failed: {error}")
        if attempt < MAX_RETRIES:
            backoff = 2 ** attempt
            logger.info(f"Retrying in {backoff}s...")
            time.sleep(backoff)
    else:
        logger.error("All fetch attempts exhausted — skipping this run.")
        return

    # VALIDATE
    valid_records, dropped = validate_records(raw_records)
    logger.info(f"Validation → valid={len(valid_records)}, dropped={dropped}")

    if not valid_records:
        logger.warning("No valid records after validation — skipping.")
        return

    # TRANSFORM
    vwap = calculate_vwap(valid_records)
    enriched = enrich_records(valid_records, vwap)
    outlier_count = sum(1 for r in enriched if r.is_outlier)

    # LOAD
    written, skipped = upsert_records(enriched)

    elapsed = round(time.time() - start, 3)

    # Structured summary log
    summary = {
        "event":                       "pipeline_run_complete",
        "records_fetched":             len(raw_records),
        "records_processed":           len(valid_records),
        "records_dropped_validation":  dropped,
        "records_written":             written,
        "records_skipped_duplicate":   skipped,
        "outliers_flagged":            outlier_count,
        "execution_time_seconds":      elapsed,
        "vwap_snapshot":               {k: round(v, 4) for k, v in vwap.items()},
    }
    logger.info(f"SUMMARY | {json.dumps(summary)}")


# ── Polling loop ──────────────────────────────────────────────────────────────

def main():
    logger.info("ETL service starting up...")
    logger.info(f"API_URL={API_URL} | POLL_INTERVAL={POLL_INTERVAL}s")

    logger.info(f"Waiting {DB_READY_WAIT}s for DB to be ready...")
    time.sleep(DB_READY_WAIT)

    init_db()
    logger.info("Database ready. Entering polling loop.")

    while True:
        try:
            run_pipeline()
        except Exception as e:
            logger.exception(f"Unhandled error in pipeline run: {e}")

        logger.info(f"Sleeping {POLL_INTERVAL}s before next run...\n")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()