import logging
from collections import defaultdict
from typing import List, Tuple

import requests
from pydantic import ValidationError

from models import EnrichedRecord, MarketRecord

logger = logging.getLogger(__name__)

OUTLIER_THRESHOLD = 0.15  # flag if price deviates >15% from batch average


# ── EXTRACT ───────────────────────────────────────────────────────────────────

def fetch_data(api_url: str, timeout: int = 10) -> Tuple[List[dict], str | None]:
    """
    Fetch raw records from the API.
    Returns (records, error_message). On success error_message is None.
    """
    try:
        response = requests.get(api_url, timeout=timeout)
        response.raise_for_status()
        return response.json(), None
    except requests.exceptions.Timeout:
        return [], "Request timed out"
    except requests.exceptions.HTTPError as e:
        return [], f"HTTP {e.response.status_code} error from API"
    except requests.exceptions.RequestException as e:
        return [], f"Network error: {str(e)}"


# ── VALIDATE ──────────────────────────────────────────────────────────────────

def validate_records(raw_records: List[dict]) -> Tuple[List[MarketRecord], int]:
    """
    Run Pydantic validation on each raw record.
    Returns (valid_records, dropped_count).
    """
    valid: List[MarketRecord] = []
    dropped = 0

    for raw in raw_records:
        try:
            valid.append(MarketRecord(**raw))
        except (ValidationError, TypeError, ValueError) as e:
            dropped += 1
            logger.warning(
                f"Dropped record [instrument={raw.get('instrument_id', '?')}]: {e}"
            )

    return valid, dropped


# ── TRANSFORM ─────────────────────────────────────────────────────────────────

def calculate_vwap(records: List[MarketRecord]) -> dict[str, float]:
    """
    VWAP = Σ(price × volume) / Σ(volume)  per instrument
    """
    pv_sum = defaultdict(float)
    vol_sum = defaultdict(float)

    for r in records:
        pv_sum[r.instrument_id] += r.price * r.volume
        vol_sum[r.instrument_id] += r.volume

    return {
        inst: pv_sum[inst] / vol_sum[inst]
        for inst in pv_sum
        if vol_sum[inst] > 0
    }


def enrich_records(
    records: List[MarketRecord],
    vwap: dict[str, float],
) -> List[EnrichedRecord]:
    """
    Attach VWAP and outlier flag to each record.
    """
    # Simple batch average per instrument as outlier baseline
    price_lists: dict[str, list[float]] = defaultdict(list)
    for r in records:
        price_lists[r.instrument_id].append(r.price)

    batch_avg = {
        inst: sum(prices) / len(prices)
        for inst, prices in price_lists.items()
    }

    enriched: List[EnrichedRecord] = []
    for r in records:
        avg = batch_avg.get(r.instrument_id, r.price)
        deviation = abs(r.price - avg) / avg if avg != 0 else 0
        is_outlier = deviation > OUTLIER_THRESHOLD

        if is_outlier:
            logger.warning(
                f"Outlier | {r.instrument_id} | price={r.price:.4f} "
                f"avg={avg:.4f} deviation={deviation:.2%}"
            )

        enriched.append(
            EnrichedRecord(
                instrument_id=r.instrument_id,
                price=r.price,
                volume=r.volume,
                timestamp=r.timestamp,
                vwap=round(vwap.get(r.instrument_id, r.price), 6),
                is_outlier=is_outlier,
            )
        )

    return enriched