"""Gold batch job: read Silver, summarize it, write Gold.

Groups the clean prices by company and day, then builds one tidy summary row per
company per day. Gold is the ready-to-serve table that analysts read.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")

SILVER_PATH = "s3a://silver/stock-prices"
GOLD_PATH = "s3a://gold/daily-summary"


def build_spark():
    return (
        SparkSession.builder
        .appName("stock-gold-batch")
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

    silver = spark.read.parquet(SILVER_PATH)
    print("silver rows:", silver.count())

    # Summarize the clean prices: one row per company per day.
    gold = (
        silver
        .groupBy("symbol", "date")
        .agg(
            F.max("high").alias("day_high"),
            F.min("low").alias("day_low"),
            F.avg("close").alias("avg_close"),
            F.sum("volume").alias("total_volume"),
        )
    )

    gold.show()
    (
        gold.write
        .format("parquet")
        .mode("overwrite")
        .partitionBy("date")
        .save(GOLD_PATH)
    )


if __name__ == "__main__":
    main()
