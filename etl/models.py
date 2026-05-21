from datetime import datetime
from pydantic import BaseModel, field_validator, ConfigDict


class MarketRecord(BaseModel):
    """Validates raw incoming record from the API."""
    instrument_id: str
    price: float
    volume: float
    timestamp: datetime

    @field_validator("price", "volume", mode="before")
    @classmethod
    def must_be_numeric_and_positive(cls, v):
        v = float(v)  # raises ValueError if v is "CORRUPT_VALUE"
        if v <= 0:
            raise ValueError(f"Expected positive number, got {v}")
        return v

    @field_validator("instrument_id")
    @classmethod
    def must_not_be_empty(cls, v):
        if not v or not v.strip():
            raise ValueError("instrument_id must not be empty")
        return v.strip().upper()


class EnrichedRecord(BaseModel):
    """Record after transformation — ready to write to DB."""
    model_config = ConfigDict(arbitrary_types_allowed=True)

    instrument_id: str
    price: float
    volume: float
    timestamp: datetime
    vwap: float
    is_outlier: bool