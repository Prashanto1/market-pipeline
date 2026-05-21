markdown# Market Data Pipeline

An end-to-end data engineering project simulating a real-time financial data feed,
with a FastAPI source, PostgreSQL sink, and a Python ETL pipeline — all containerized
with Docker Compose.

---

## Architecture
┌──────────────┐     HTTP GET      ┌──────────────┐     psycopg2     ┌──────────────┐
│  FastAPI     │ ◄──────────────── │  ETL Service │ ───────────────► │  PostgreSQL  │
│  (Port 8000) │  /v1/market-data  │  (Python)    │                  │  (Port 5432) │
└──────────────┘                   └──────────────┘                  └──────────────┘
▲ 5% fault injection               │
│ (500 error or corrupt data)       │ Every 5 seconds
└───────────────────────────────────┘
Docker Internal Network (market_net)

## Quick Start

```bash
# Clone the repo
git clone <your-repo-url>
cd market-pipeline

# Spin up everything
docker compose up --build

# Verify API
curl http://localhost:8000/health
curl http://localhost:8000/v1/market-data

# View live ETL logs
docker logs -f market_etl

# Query the database
docker exec -it market_db psql -U etl_user -d market_data \
  -c "SELECT instrument_id, price, vwap, is_outlier, timestamp FROM market_data ORDER BY timestamp DESC LIMIT 10;"

# Stop
docker compose down
```

---

## Project Structure
market-pipeline/
├── docker-compose.yml
├── .env
├── .gitignore
├── README.md
├── api/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py
└── etl/
├── Dockerfile
├── requirements.txt
├── models.py
├── pipeline.py
├── db.py
└── main.py

---

## ETL Pipeline Detail

### Extract
- Polls `/v1/market-data` every 5 seconds
- Exponential backoff retry on failure (2s → 4s → 8s, max 3 attempts)
- Handles HTTP 500, timeouts, and network errors

### Validate (Pydantic)
- Rejects records where `price` or `volume` is non-numeric or non-positive
- Catches all chaos-injected corrupt values
- Logs every dropped record with reason

### Transform
- **VWAP** = `Σ(price × volume) / Σ(volume)` per `instrument_id`
- **Outlier flag** = price deviates >15% from batch average for that instrument

### Load
- `INSERT ... ON CONFLICT (instrument_id, timestamp) DO NOTHING`
- Guarantees no duplicate rows even if pipeline reruns on same data

### Structured Log Output (per run)
```json
{
  "event": "pipeline_run_complete",
  "records_fetched": 8,
  "records_processed": 7,
  "records_dropped_validation": 1,
  "records_written": 7,
  "records_skipped_duplicate": 0,
  "outliers_flagged": 0,
  "execution_time_seconds": 0.051,
  "vwap_snapshot": {
    "AAPL": 180.2625,
    "BTC-USD": 65864.4564
  }
}
```

---

## System Design Q&A

### 1. Scaling to 1 Billion Events/Day

At ~11,500 events/second sustained, the current polling architecture breaks down.

**Ingestion** — Replace polling with **Apache Kafka**. The API publishes events to
a Kafka topic; the ETL becomes a consumer group. Kafka handles backpressure and
allows multiple consumers in parallel.

**Processing** — Replace the single-threaded Python script with **Apache Spark
Structured Streaming** or **Flink** for distributed parallel computation across
a cluster.

**Storage** — PostgreSQL won't sustain this write throughput. Switch to a columnar
time-series store like **TimescaleDB**, **ClickHouse**, or cloud-native options
like **BigQuery** / **Redshift**.

**Cloud shortcut** — AWS: Kinesis → Glue → Redshift. GCP: Pub/Sub → Dataflow → BigQuery.

---

### 2. Production Health Monitoring

**Infrastructure level** — Docker healthchecks (already implemented). In Kubernetes
this maps to `livenessProbe` and `readinessProbe`.

**Pipeline level** — Emit metrics after each run (last success timestamp, records
processed, error rate) to **Prometheus**, visualized in **Grafana**. Alert if
`last_successful_run` is older than 2× the poll interval.

**Data quality level** — Track validation drop rate over time. If >20% of records
are dropped in a window, alert — it signals a schema change upstream.

**Dead man's switch** — ETL writes a heartbeat timestamp to a `pipeline_health`
table after each run. A separate monitor pages on silence.

---

### 3. Idempotency and Mid-Batch Recovery

The current design is already idempotent at the row level:
```sql
ON CONFLICT (instrument_id, timestamp) DO NOTHING
```
Rerunning the pipeline on the same data is always safe.

For a 10GB batch:

**Checkpointing** — Track the last successfully processed offset (Kafka offset or
batch ID) in a `pipeline_checkpoints` table. On restart, resume from the last
committed checkpoint rather than replaying the full batch.

**Transactional writes** — Wrap each micro-batch in a DB transaction. A mid-write
crash rolls back cleanly — no partial data lands in the DB.

**Exactly-once semantics** — In Kafka, use producer transactions combined with
`enable.idempotence=true` to guarantee each message is written exactly once across
restarts.

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `POSTGRES_DB` | `market_data` | Database name |
| `POSTGRES_USER` | `etl_user` | DB username |
| `POSTGRES_PASSWORD` | — | DB password |
| `POSTGRES_HOST` | `db` | DB hostname |
| `POSTGRES_PORT` | `5432` | DB port |
| `API_URL` | `http://api:8000/v1/market-data` | Market data endpoint |
| `POLL_INTERVAL_SECONDS` | `5` | How often ETL polls |
| `MAX_RETRIES` | `3` | Fetch retry attempts |

