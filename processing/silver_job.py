"""Silver batch job: read Bronze, clean it, write Silver.

Bronze is the raw copy (with duplicates and any bad rows). This job washes it:
it removes duplicate rows and drops bad prices, then saves the clean result to
the Silver bucket. Silver is the trustworthy copy other steps can rely on.
"""

import os

from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")

BRONZE_PATH = "s3a://bronze/stock-prices"
SILVER_PATH = "s3a://silver/stock-prices"


def build_spark():
    return (
        SparkSession.builder
        .appName("stock-silver-batch")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def clean(bronze):
    """Keep one row per (symbol, timestamp) natural key and drop non-positive prices."""
    return bronze.dropDuplicates(["symbol", "timestamp"]).filter("close > 0")


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    bronze = spark.read.parquet(BRONZE_PATH)
    print("bronze rows:", bronze.count())

    cleaned = clean(bronze)

    print("silver rows:", cleaned.count())
    (
        cleaned.write
        .format("parquet")
        .mode("overwrite")
        .partitionBy("date")
        .save(SILVER_PATH)
    )


if __name__ == "__main__":
    main()
