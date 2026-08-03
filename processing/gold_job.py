"""Gold batch job: build a star schema from Silver.

Produces three tables in the gold bucket:
  - dim_date           one row per calendar date, with date attributes
  - dim_ticker         one row per ticker, with descriptive attributes
  - fact_daily_prices  one row per (ticker, date) with the measures, referencing the dims

The fact table holds only measures plus surrogate keys (date_key, ticker_key) that point
at the dimensions. This is the standard star schema used in analytics warehouses.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")

SILVER_PATH = "s3a://silver/stock-prices"
GOLD = "s3a://gold"

# Descriptive attributes for the ticker dimension (a real dimension carries context,
# not just the code). Unknown symbols simply get a null company_name.
COMPANY_NAMES = {
    "AAPL": "Apple Inc.",
    "MSFT": "Microsoft Corporation",
    "GOOGL": "Alphabet Inc.",
    "AMZN": "Amazon.com Inc.",
}


def build_spark():
    return (
        SparkSession.builder
        .appName("stock-gold-star-schema")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def build_dim_date(silver):
    return (
        silver.select("date").distinct()
        .withColumn("date_key", F.regexp_replace("date", "-", "").cast("int"))  # 20260730
        .withColumn("full_date", F.to_date("date"))
        .withColumn("year", F.year("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("day", F.dayofmonth("full_date"))
        .withColumn("day_of_week", F.date_format("full_date", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("full_date").isin(1, 7))  # 1=Sun, 7=Sat
        .select("date_key", "full_date", "year", "month", "day", "day_of_week", "is_weekend")
    )


def build_dim_ticker(silver):
    pairs = []
    for symbol, name in COMPANY_NAMES.items():
        pairs += [F.lit(symbol), F.lit(name)]
    company_map = F.create_map(*pairs)
    return (
        silver.select("symbol", "currency", "source").distinct()
        .withColumn("company_name", company_map[F.col("symbol")])
        # surrogate key: a simple 1..N id, independent of the business key (symbol)
        .withColumn("ticker_key", F.row_number().over(Window.orderBy("symbol")))
        .select("ticker_key", "symbol", "company_name", "currency", "source")
    )


def build_fact(silver, dim_ticker):
    daily = (
        silver.groupBy("symbol", "date")
        .agg(
            F.max("high").alias("day_high"),
            F.min("low").alias("day_low"),
            F.avg("close").alias("avg_close"),
            F.sum("volume").alias("total_volume"),
        )
    )
    return (
        daily
        .join(dim_ticker.select("ticker_key", "symbol"), on="symbol", how="left")
        .withColumn("date_key", F.regexp_replace("date", "-", "").cast("int"))
        .select("date_key", "ticker_key", "day_high", "day_low", "avg_close", "total_volume")
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    silver = spark.read.parquet(SILVER_PATH)

    dim_date = build_dim_date(silver)
    dim_ticker = build_dim_ticker(silver)
    fact = build_fact(silver, dim_ticker)

    dim_ticker.show(truncate=False)
    dim_date.show(truncate=False)
    fact.show(truncate=False)

    dim_date.write.mode("overwrite").parquet(f"{GOLD}/dim_date")
    dim_ticker.write.mode("overwrite").parquet(f"{GOLD}/dim_ticker")
    fact.write.mode("overwrite").parquet(f"{GOLD}/fact_daily_prices")
    print("wrote dim_date, dim_ticker, fact_daily_prices to the gold bucket")


if __name__ == "__main__":
    main()
