# Architecture

## Overview

The pipeline ingests live stock market data, processes it through a layered
(bronze / silver / gold) data lake, and loads analytics ready tables into Snowflake.

```
Stock API (Yahoo Finance / Alpha Vantage)
  -> Python producer
  -> Kafka (with Zookeeper)
  -> Spark Structured Streaming
  -> Bronze layer (MinIO, raw Parquet)
  -> Silver layer (cleaned, validated)
  -> Gold layer (star schema)
  -> Snowflake
  -> Analytics SQL
```

Airflow orchestrates the batch jobs, including retries, backfills, and task dependencies.
All services run locally with Docker Compose, and PostgreSQL backs the Airflow metadata.

## Components

* Python producer: polls the stock API and publishes events to a Kafka topic.
* Kafka and Zookeeper: a durable, ordered, replayable event log.
* Spark Structured Streaming: consumes Kafka and writes raw events to the bronze layer.
* MinIO: S3 compatible object storage for the data lake.
* Spark batch jobs: clean bronze into silver and model silver into a gold star schema.
* Airflow: orchestrates the batch jobs.
* Snowflake: the analytics warehouse.

## Data contract: stock event

Each Kafka message represents one observation of one ticker at one point in time. All
timestamps are UTC (ISO-8601). Prices are decimal.

| Field | Type | Description |
| ----- | ---- | ----------- |
| symbol | string | ticker, e.g. AAPL |
| timestamp | string | when the observation is for (event time) |
| open | decimal | opening price of the bar |
| high | decimal | highest price of the bar |
| low | decimal | lowest price of the bar |
| close | decimal | closing price of the bar |
| volume | integer | shares traded in the bar |
| source | string | data provenance, e.g. yfinance |
| interval | string | bar resolution, e.g. 1m |
| currency | string | price units, e.g. USD |
| ingested_at | string | when the producer fetched it (processing time) |

The natural key is (symbol, timestamp): it uniquely identifies one observation and is used
downstream for deduplication and idempotency. The producer polls each ticker on a
configurable interval (default 60 seconds).

## Storage

MinIO exposes an S3 compatible API on port 9000 and a web console on port 9001. Objects are
stored in a Docker named volume so data persists across container restarts.

Configuration is injected through environment variables. Secrets are never committed to
source control.
