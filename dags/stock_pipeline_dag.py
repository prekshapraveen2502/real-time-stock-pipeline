"""Airflow DAG: refine the stock data on a schedule.

Runs the batch steps in order: clean (silver) -> summarize (gold) -> load into the
warehouse. Ingestion (producer -> Kafka -> bronze) runs continuously as its own
services; this DAG schedules the refinement on top of the bronze data.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from alerts import notify_failure  # Airflow puts the dags folder on the path

# Project root is derived from this file's location (this DAG lives in <project>/dags), so
# there is no hardcoded machine path. In the Airflow container that is /app.
PROJECT_DIR = os.getenv(
    "PROJECT_DIR",
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
)


def spark_cmd(script, extra_args=""):
    # The Airflow image is built on the Spark image, so PySpark, Java and the Kafka, S3 and
    # Postgres jars are already installed; a job is just a Python script.
    tail = f" {extra_args}" if extra_args else ""
    return f'cd "{PROJECT_DIR}" && python {script}{tail}'


default_args = {
    "retries": 3,                          # retry transient failures (e.g. storage commit hiccups)
    "retry_delay": timedelta(seconds=30),
    # Retries only help transient failures. A failure that repeats on every attempt (like a broken
    # environment) needs a person, so alert once the last retry has failed.
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="stock_pipeline",
    description="Refine stock data: silver -> gold -> warehouse",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",                   # run once a day
    catchup=False,                       # don't backfill past days
    default_args=default_args,
    tags=["stocks"],
) as dag:

    silver = BashOperator(
        task_id="silver_clean",
        bash_command=spark_cmd("processing/silver_job.py"),
    )

    gold = BashOperator(
        task_id="gold_summary",
        # {{ ds }} is Airflow's logical batch date; passing it keeps SCD2 boundaries tied to the
        # logical run, not the physical execution day (so a retry the next day is unaffected).
        bash_command=spark_cmd("processing/gold_job.py", "--batch-date {{ ds }}"),
    )

    load = BashOperator(
        task_id="load_warehouse",
        bash_command=spark_cmd("warehouse/load_to_warehouse.py"),
    )

    silver >> gold >> load
