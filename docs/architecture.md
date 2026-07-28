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

## Storage

MinIO exposes an S3 compatible API on port 9000 and a web console on port 9001. Objects are
stored in a Docker named volume so data persists across container restarts.

Configuration is injected through environment variables. Secrets are never committed to
source control.
