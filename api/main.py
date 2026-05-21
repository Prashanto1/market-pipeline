import random
from datetime import datetime, timezone

from fastapi import FastAPI, Response

app = FastAPI(title="Mock Market Data API")

INSTRUMENTS = ["AAPL", "GOOGL", "MSFT", "BTC-USD", "ETH-USD", "TSLA", "AMZN", "NFLX"]

BASE_PRICES = {
    "AAPL": 180.0,
    "GOOGL": 140.0,
    "MSFT": 380.0,
    "BTC-USD": 65000.0,
    "ETH-USD": 3500.0,
    "TSLA": 250.0,
    "AMZN": 185.0,
    "NFLX": 620.0,
}


def generate_clean_data() -> list[dict]:
    records = []
    for instrument in INSTRUMENTS:
        base = BASE_PRICES[instrument]
        price = round(base * (1 + random.uniform(-0.02, 0.02)), 4)
        volume = round(random.uniform(100, 10000), 2)
        records.append({
            "instrument_id": instrument,
            "price": price,
            "volume": volume,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
    return records


@app.get("/v1/market-data")
def get_market_data(response: Response):
    chaos_roll = random.random()

    # 2.5% → hard 500 error
    if chaos_roll < 0.025:
        response.status_code = 500
        return {"error": "Fault Injected: Internal Server Error"}

    # 2.5% → malformed payload
    if chaos_roll < 0.05:
        data = generate_clean_data()
        corrupt_idx = random.randint(0, len(data) - 1)
        corrupt_field = random.choice(["price", "volume"])
        data[corrupt_idx][corrupt_field] = "CORRUPT_VALUE"
        return data

    # 95% → clean data
    return generate_clean_data()


@app.get("/health")
def health_check():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}