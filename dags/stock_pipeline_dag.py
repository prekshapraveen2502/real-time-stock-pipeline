"""Airflow DAG: refine the stock data on a schedule.

Runs the batch steps in order: clean (silver) -> summarize (gold) -> load into the
warehouse. Ingestion (producer -> Kafka -> bronze) runs continuously as its own
services; this DAG schedules the refinement on top of the bronze data.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/Users/prekshapraveen/Desktop/Real-Time Stock Data Engineering Project"
VENV_BIN = f"{PROJECT_DIR}/venv/bin"
S3_PKG = "org.apache.hadoop:hadoop-aws:3.5.0"
PG_PKG = "org.postgresql:postgresql:42.7.4"


def spark_cmd(script, packages):
    # Put the project venv first on PATH so spark-submit uses the Python that has PySpark.
    # arch -arm64 forces native mode (Airflow runs on an Intel Python; our packages are arm64).
    return (
        f'cd "{PROJECT_DIR}" && export PATH="{VENV_BIN}:$PATH" && '
        f'arch -arm64 spark-submit --packages {packages} {script}'
    )


default_args = {
    "retries": 3,                          # retry transient failures (e.g. storage commit hiccups)
    "retry_delay": timedelta(seconds=30),
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
        bash_command=spark_cmd("processing/silver_job.py", S3_PKG),
    )

    gold = BashOperator(
        task_id="gold_summary",
        bash_command=spark_cmd("processing/gold_job.py", S3_PKG),
    )

    load = BashOperator(
        task_id="load_warehouse",
        bash_command=spark_cmd("warehouse/load_to_warehouse.py", f"{S3_PKG},{PG_PKG}"),
    )

    silver >> gold >> load
