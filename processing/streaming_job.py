"""Spark Structured Streaming: read stock events from Kafka and land them in Bronze.

For every Kafka event we first preserve the raw payload and Kafka metadata, then parse it
with the fixed STOCK_SCHEMA and run basic data-quality checks. Records that pass go to the
bronze bucket; records that fail (bad JSON, missing fields, bad numbers, or broken OHLC
business rules) go to a separate quarantine bucket with an error_reason, so no bad record is
silently dropped and every raw event is recoverable for investigation.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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
QUARANTINE_PATH = "s3a://quarantine/stock-prices"

# Each streaming query needs its OWN checkpoint directory. A checkpoint stores a query's
# Kafka offsets and file-sink state; two queries sharing one directory would corrupt each
# other's progress. Both are local paths (same as before).
BRONZE_CHECKPOINT = "spark-checkpoints/bronze"
QUARANTINE_CHECKPOINT = "spark-checkpoints/quarantine"

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

# Parsed fields we keep on every row (present for valid rows, possibly null for quarantined).
PARSED_FIELDS = [f.name for f in STOCK_SCHEMA.fields]


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


def validated_stream(spark):
    """Read Kafka, preserve raw + metadata, parse, and attach an error_reason column."""
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    # 1. Preserve the original event and Kafka metadata BEFORE parsing.
    events = raw.select(
        F.col("value").cast("string").alias("raw_payload"),
        F.col("key").cast("string").alias("kafka_key"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("bronze_ingested_at"),
    )

    # 2. Parse the JSON payload. from_json returns a NULL struct if the JSON is malformed.
    parsed = events.withColumn("data", F.from_json("raw_payload", STOCK_SCHEMA))
    flat = parsed.select(
        "raw_payload", "kafka_key", "kafka_topic", "kafka_partition", "kafka_offset",
        "kafka_timestamp", "bronze_ingested_at",
        # malformed = JSON did not parse, or the payload is not even a JSON object.
        (F.col("data").isNull() | ~F.trim(F.col("raw_payload")).startswith("{")).alias("_malformed"),
        *[F.col(f"data.{name}").alias(name) for name in PARSED_FIELDS],
    )

    # 3. Data-quality checks. Each check emits a label when it fails; concat_ws joins the
    #    failed labels (and skips the nulls), so error_reason lists every rule that failed,
    #    or is "" when the row is fully valid.
    ok = ~F.col("_malformed")  # JSON parsed
    error_reason = F.concat_ws(
        ",",
        F.when(F.col("_malformed"), F.lit("malformed_json")),
        F.when(ok & F.col("symbol").isNull(), F.lit("missing_symbol")),
        F.when(ok & F.col("timestamp").isNull(), F.lit("missing_timestamp")),
        F.when(
            ok & (
                F.col("open").isNull() | F.col("high").isNull() | F.col("low").isNull()
                | F.col("close").isNull() | F.col("volume").isNull()
            ),
            F.lit("invalid_numeric"),
        ),
        F.when(F.col("close") <= 0, F.lit("close<=0")),
        F.when(F.col("volume") < 0, F.lit("volume<0")),
        F.when(F.col("high") < F.col("low"), F.lit("high<low")),
        F.when(F.col("high") < F.col("open"), F.lit("high<open")),
        F.when(F.col("high") < F.col("close"), F.lit("high<close")),
        F.when(F.col("low") > F.col("open"), F.lit("low>open")),
        F.when(F.col("low") > F.col("close"), F.lit("low>close")),
    )
    return flat.withColumn("error_reason", error_reason).drop("_malformed")


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    checked = validated_stream(spark)

    # 4. Split. Valid rows are close to the source (no aggregation/dedup here); they are
    #    partitioned by event date. Quarantine rows keep the raw payload, metadata, parsed
    #    fields, and error_reason, partitioned by ingestion date (a bad row may have no
    #    usable event timestamp).
    valid = (
        checked.filter(F.col("error_reason") == "")
        .drop("error_reason")
        .withColumn("date", F.col("timestamp").substr(1, 10))
    )
    quarantine = (
        checked.filter(F.col("error_reason") != "")
        .withColumn("date", F.date_format(F.col("bronze_ingested_at"), "yyyy-MM-dd"))
    )

    # 5 + 6. Two independent streaming queries, each with its OWN checkpoint.
    bronze_query = (
        valid.writeStream
        .format("parquet")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", BRONZE_CHECKPOINT)
        .partitionBy("date")
        .outputMode("append")
        .start()
    )
    quarantine_query = (
        quarantine.writeStream
        .format("parquet")
        .option("path", QUARANTINE_PATH)
        .option("checkpointLocation", QUARANTINE_CHECKPOINT)
        .partitionBy("date")
        .outputMode("append")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
