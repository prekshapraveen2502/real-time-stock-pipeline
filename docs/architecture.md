# Architecture

This document grows as the project is built, one phase at a time.

## Target end-to-end pipeline

```
Stock API (Yahoo Finance / Alpha Vantage)
        │  Python Producer (polling, retries, backoff, keyed messages, idempotency)
        ▼
   Kafka Topic  ◄── Zookeeper
        │  Spark Structured Streaming (checkpointing, watermarks)
        ▼
   Bronze Layer (MinIO, raw immutable Parquet)
        │  clean / dedupe / data-quality checks / schema enforcement
        ▼
   Silver Layer (MinIO, validated)
        │  dimensional modeling
        ▼
   Gold Layer (star schema: fact_prices + dim_ticker/dim_date, SCD2, surrogate keys)
        │  COPY INTO / stages
        ▼
   Snowflake  →  Analytics SQL  →  Dashboard (optional)

Airflow orchestrates the batch path (retries, backfills, sensors, dependencies).
Everything runs via docker-compose; Postgres backs Airflow metadata.
```

## Component responsibilities

| Component | Responsibility | Phase |
|-----------|----------------|------:|
| Python Producer | Poll the stock API, publish keyed JSON events to Kafka | 2 |
| Kafka + Zookeeper | Durable, ordered, replayable event log | 3 |
| Spark Structured Streaming | Consume Kafka, write raw events to Bronze | 4 |
| MinIO | S3-compatible object storage for Bronze/Silver/Gold | 5 |
| Spark (batch) | Bronze→Silver cleaning, Silver→Gold modeling | 5–7 |
| Snowflake | Analytics warehouse (star schema) | 9 |
| Airflow | Orchestrate batch jobs, retries, backfills | 8 |

## Current state — after Phase 0

Only local infrastructure exists so far:

```
Docker Compose
   └── MinIO (object storage)
         :9000  S3 API
         :9001  web console
         volume: minio_data  (persists across restarts)
```

Config is injected from a gitignored `.env` file (12-Factor); secrets never live in code.
