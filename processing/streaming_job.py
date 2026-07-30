"""Spark Structured Streaming: read stock events from Kafka and save them to Bronze.

Reads the raw messages from the Kafka topic, parses our JSON data contract, and
writes them unchanged into the MinIO bronze bucket as Parquet files, partitioned
by date. This is the bronze layer: the raw, untouched copy of the data.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType
from dotenv import load_dotenv

load_dotenv()  # read .env so config matches the rest of the project

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock-prices")

# MinIO (S3-compatible storage) connection details.
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")

BRONZE_PATH = "s3a://bronze/stock-prices"
CHECKPOINT_PATH = "spark-checkpoints/bronze"  # local: remembers which offsets are done

STOCK_SCHEMA = StructType([
    StructField("symbol", StringType()),
    StructField("timestamp", StringType()),
    StructField("open", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),
    StructField("volume", LongType()),
    StructField("source", StringType()),
    StructField("interval", StringType()),
    StructField("currency", StringType()),
    StructField("ingested_at", StringType()),
])


def build_spark():
    # These fs.s3a.* settings tell Spark how to reach MinIO instead of real AWS S3.
    return (
        SparkSession.builder
        .appName("stock-bronze-stream")
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

    # Read the raw stream from Kafka (key/value arrive as raw bytes).
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Turn the Kafka value (bytes) into real columns, then add a date column
    # so files are grouped into per-day folders.
    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json")
        .select(from_json(col("json"), STOCK_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("date", col("timestamp").substr(1, 10))  # 2026-07-30
    )

    # Write the raw rows to the bronze bucket as Parquet, one folder per date.
    query = (
        parsed.writeStream
        .format("parquet")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("date")
        .outputMode("append")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
