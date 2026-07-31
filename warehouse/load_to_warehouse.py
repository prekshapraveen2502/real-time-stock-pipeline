"""Load the Gold summary from MinIO into the Postgres warehouse.

Spark reads the Gold Parquet files and writes them into a Postgres table that
analysts can query with SQL. This is our local stand-in for loading into
Snowflake; the shape of the job would be the same either way.
"""

import os

from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")

GOLD_PATH = "s3a://gold/daily-summary"

PG_HOST = os.getenv("WAREHOUSE_HOST", "localhost")
PG_PORT = os.getenv("WAREHOUSE_PORT", "5432")
PG_DB = os.getenv("WAREHOUSE_DB", "stock_warehouse")
PG_USER = os.getenv("WAREHOUSE_USER", "warehouse")
PG_PASSWORD = os.getenv("WAREHOUSE_PASSWORD", "warehouse123")
JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
TABLE = "daily_summary"


def build_spark():
    return (
        SparkSession.builder
        .appName("load-gold-to-warehouse")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    gold = spark.read.parquet(GOLD_PATH)
    print("gold rows to load:", gold.count())

    (
        gold.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", TABLE)
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")  # replace the table each run (idempotent for our small data)
        .save()
    )
    print(f"loaded into postgres table '{TABLE}'")


if __name__ == "__main__":
    main()
