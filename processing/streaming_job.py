"""Spark Structured Streaming: read stock events from Kafka and print them.

Step A: prove Spark can consume the topic and parse our JSON data contract.
Step B (next) will switch the sink to the MinIO bronze layer as Parquet.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock-prices")


# =====================================================================
# TODO 1 (YOUR CODE): define the schema of one stock event.
# It must match our data contract. Fill in every field with the right type:
#   - text fields  -> StringType()      (symbol, timestamp, source, interval,
#                                         currency, ingested_at)
#   - price fields -> DoubleType()      (open, high, low, close)
#   - volume       -> LongType()
# Example of the pattern:  StructField("symbol", StringType()),
# =====================================================================
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
    return SparkSession.builder.appName("stock-bronze-stream").getOrCreate()


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Read the raw stream from Kafka. Kafka delivers key/value as raw bytes.
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    # =================================================================
    # TODO 2 (YOUR CODE): turn the raw Kafka "value" (bytes) into columns.
    # Three moves:
    #   a) cast the `value` column to a string
    #   b) parse that JSON string with STOCK_SCHEMA (hint: from_json)
    #   c) flatten it so each field is its own top-level column
    # Replace this placeholder with your transformation.
    # =================================================================
    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json")               # a) bytes -> string
        .select(from_json(col("json"), STOCK_SCHEMA).alias("data"))   # b) decode with schema
        .select("data.*")                                             # c) spread into columns
    )

    # Print each micro-batch to the console so we can watch it work.
    query = (
        parsed.writeStream
        .format("console")
        .option("truncate", "false")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
