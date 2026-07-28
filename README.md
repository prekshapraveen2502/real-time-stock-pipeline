# Real-Time Stock Data Engineering Pipeline

An end-to-end streaming data pipeline that ingests live stock market data, processes it in
real time, and lands it in a cloud warehouse for analytics.

**Stack:** Python · Kafka · Spark Structured Streaming · Airflow · MinIO · PostgreSQL ·
Snowflake · Docker · Git

## Architecture (target)

```
Stock API → Python Producer → Kafka → Spark Streaming
   → Bronze (MinIO) → Silver → Gold (star schema) → Snowflake → Analytics SQL
```

Orchestrated by Airflow; all services run locally via Docker Compose. See
[docs/architecture.md](docs/architecture.md) for detail.

## Project status

Built in phases. Current: **Phase 0 — Environment Setup** (done).

| Phase | Focus | Status |
|------:|-------|--------|
| 0 | Environment: Docker, venv, config/secrets, Git | ✅ |
| 1 | Architecture design | ⬜ |
| 2 | Stock data producer | ⬜ |
| 3 | Kafka | ⬜ |
| 4 | Spark Structured Streaming | ⬜ |
| 5–7 | Bronze / Silver / Gold layers | ⬜ |
| 8 | Airflow orchestration | ⬜ |
| 9 | Snowflake | ⬜ |
| 10–14 | Testing, monitoring, optimization, CI/CD, polish | ⬜ |

## Getting started

```bash
# 1. Python environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Configuration (copy the template, then fill in real values)
cp .env.example .env

# 3. Start the local stack (currently: MinIO object storage)
docker compose up -d

# MinIO console:  http://localhost:9001   (creds from your .env)
```

Tear down with `docker compose down` (add `-v` to also delete stored data).

## Repository layout

```
ingestion/    stock data producer & consumers
processing/   Spark streaming + batch (bronze→silver→gold)
dags/         Airflow DAGs
warehouse/    Snowflake DDL + analytics SQL
quality/      data quality checks
tests/        pytest suite
config/        application configuration
docs/         architecture, diagrams, phase reflections
```
