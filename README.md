# Real-Time Stock Data Engineering Pipeline

An end to end streaming pipeline that ingests live stock market data, processes it in real
time, and loads it into a cloud warehouse for analytics.

## Tech stack

Python, Kafka, Spark Structured Streaming, Airflow, MinIO, PostgreSQL, Snowflake, Docker.

## Architecture

```
Stock API -> Python Producer -> Kafka -> Spark Structured Streaming
   -> Bronze (MinIO) -> Silver -> Gold -> Snowflake -> Analytics SQL
```

Airflow orchestrates the batch jobs. All services run locally with Docker Compose. See
docs/architecture.md for detail.

## Getting started

Prerequisites: Docker Desktop and Python 3.

```bash
# Python environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Configuration: copy the template, then fill in your values
cp .env.example .env

# Start the local stack
docker compose up -d
```

MinIO console: http://localhost:9001 (credentials from your .env). Stop the stack with
`docker compose down`.

## Repository layout

```
ingestion/    stock data producer and consumers
processing/   Spark streaming and batch jobs
dags/         Airflow DAGs
warehouse/    Snowflake DDL and analytics SQL
quality/      data quality checks
tests/        test suite
config/       application configuration
docs/         architecture and diagrams
```
