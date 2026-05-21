import logging
import os
from typing import List

import psycopg2
from psycopg2.extras import execute_values

from models import EnrichedRecord

logger = logging.getLogger(__name__)


# ── CONNECTION ────────────────────────────────────────────────────────────────

def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "db"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "market_data"),
        user=os.getenv("POSTGRES_USER", "etl_user"),
        password=os.getenv("POSTGRES_PASSWORD", "etl_password"),
    )


# ── SCHEMA INIT ───────────────────────────────────────────────────────────────

def init_db():
    """Create table and indexes if they don't exist."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS market_data (
                    id            SERIAL PRIMARY KEY,
                    instrument_id VARCHAR(20)       NOT NULL,
                    price         DOUBLE PRECISION  NOT NULL,
                    volume        DOUBLE PRECISION  NOT NULL,
                    timestamp     TIMESTAMPTZ       NOT NULL,
                    vwap          DOUBLE PRECISION,
                    is_outlier    BOOLEAN           DEFAULT FALSE,
                    ingested_at   TIMESTAMPTZ       DEFAULT NOW(),

                    -- Prevents duplicate rows on reruns
                    UNIQUE (instrument_id, timestamp)
                );
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_market_instrument_ts
                ON market_data (instrument_id, timestamp DESC);
            """)
        conn.commit()
        logger.info("Database schema initialized.")
    finally:
        conn.close()


# ── UPSERT ────────────────────────────────────────────────────────────────────

def upsert_records(records: List[EnrichedRecord]) -> tuple[int, int]:
    """
    Insert records, silently skip duplicates.
    Returns (rows_written, rows_skipped).
    """
    if not records:
        return 0, 0

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            values = [
                (
                    r.instrument_id,
                    r.price,
                    r.volume,
                    r.timestamp,
                    r.vwap,
                    r.is_outlier,
                )
                for r in records
            ]
            execute_values(
                cur,
                """
                INSERT INTO market_data
                    (instrument_id, price, volume, timestamp, vwap, is_outlier)
                VALUES %s
                ON CONFLICT (instrument_id, timestamp) DO NOTHING
                """,
                values,
            )
            written = cur.rowcount
        conn.commit()
    finally:
        conn.close()

    skipped = len(records) - written
    return written, skipped