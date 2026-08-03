"""Gold batch job: build a star schema from Silver, with an SCD Type 2 ticker dimension.

Produces three tables in the gold bucket:
  - dim_date           one row per calendar date, with date attributes
  - dim_ticker         SCD Type 2: one row per version of a ticker's attributes, with
                       effective_from / effective_to / is_current so history is preserved
  - fact_daily_prices  one row per (ticker, date) with the measures, referencing the
                       CURRENT ticker version and the date dimension

The fact holds only measures plus surrogate keys (date_key, ticker_key). This is the
standard star schema used in analytics warehouses.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    BooleanType, DateType, IntegerType, StringType, StructField, StructType,
)
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")

SILVER_PATH = "s3a://silver/stock-prices"
GOLD = "s3a://gold"

HIGH_DATE = "9999-12-31"                       # "open" end date for the current version
ATTRS = ["company_name", "currency", "source"]  # tracked attributes of a ticker
DIM_TICKER_COLS = ["ticker_key", "symbol", "company_name", "currency", "source",
                   "effective_from", "effective_to", "is_current"]
DIM_TICKER_SCHEMA = StructType([
    StructField("ticker_key", IntegerType()),
    StructField("symbol", StringType()),
    StructField("company_name", StringType()),
    StructField("currency", StringType()),
    StructField("source", StringType()),
    StructField("effective_from", DateType()),
    StructField("effective_to", DateType()),
    StructField("is_current", BooleanType()),
])

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


def _incoming_ticker_attrs(silver):
    pairs = []
    for symbol, name in COMPANY_NAMES.items():
        pairs += [F.lit(symbol), F.lit(name)]
    company_map = F.create_map(*pairs)
    return (
        silver.select("symbol", "currency", "source").distinct()
        .withColumn("company_name", company_map[F.col("symbol")])
        .select("symbol", "company_name", "currency", "source")
    )


def build_dim_ticker(spark, silver):
    """SCD Type 2 merge. New/changed tickers get a new version; changed old versions are
    closed (is_current=false, effective_to=today). Unchanged and historical rows are kept."""
    incoming = _incoming_ticker_attrs(silver)

    # Load the existing dimension if present. Re-create it from collected rows so the
    # DataFrame no longer reads the file, which lets us safely overwrite the same path.
    existing = None
    try:
        rows = spark.read.parquet(f"{GOLD}/dim_ticker").collect()
        if rows:
            existing = spark.createDataFrame(rows, DIM_TICKER_SCHEMA)
    except Exception:
        existing = None

    if existing is None:
        # First load: every ticker is a brand-new current version.
        w = Window.orderBy("symbol")
        return (
            incoming
            .withColumn("ticker_key", F.row_number().over(w))
            .withColumn("effective_from", F.current_date())
            .withColumn("effective_to", F.to_date(F.lit(HIGH_DATE)))
            .withColumn("is_current", F.lit(True))
            .select(*DIM_TICKER_COLS)
        )

    current = existing.filter(F.col("is_current"))
    history = existing.filter(~F.col("is_current"))
    max_key = existing.agg(F.max("ticker_key")).first()[0] or 0

    # Compare incoming attributes to the current version of each symbol.
    cur = current.select("symbol", *[F.col(a).alias(f"cur_{a}") for a in ATTRS])
    cmp = incoming.join(cur, on="symbol", how="left")
    changed = None
    for a in ATTRS:
        cond = F.col(f"cur_{a}").isNull() | (F.col(a) != F.col(f"cur_{a}"))  # null => new symbol
        changed = cond if changed is None else (changed | cond)
    affected = cmp.filter(changed)  # attribute changes + brand-new symbols
    affected_symbols = [r["symbol"] for r in affected.select("symbol").distinct().collect()]

    # New current version for every changed/new symbol, with fresh surrogate keys.
    w = Window.orderBy("symbol")
    new_versions = (
        affected.select("symbol", *ATTRS)
        .withColumn("ticker_key", F.row_number().over(w) + F.lit(int(max_key)))
        .withColumn("effective_from", F.current_date())
        .withColumn("effective_to", F.to_date(F.lit(HIGH_DATE)))
        .withColumn("is_current", F.lit(True))
        .select(*DIM_TICKER_COLS)
    )

    if affected_symbols:
        expired = (
            current.filter(F.col("symbol").isin(affected_symbols))
            .withColumn("effective_to", F.current_date())
            .withColumn("is_current", F.lit(False))
            .select(*DIM_TICKER_COLS)
        )
        unchanged = current.filter(~F.col("symbol").isin(affected_symbols)).select(*DIM_TICKER_COLS)
    else:
        expired = spark.createDataFrame([], DIM_TICKER_SCHEMA)
        unchanged = current.select(*DIM_TICKER_COLS)

    return (
        history.select(*DIM_TICKER_COLS)
        .unionByName(unchanged)
        .unionByName(expired)
        .unionByName(new_versions)
    )


def build_fact(silver, dim_ticker):
    current = dim_ticker.filter(F.col("is_current")).select("ticker_key", "symbol")
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
        .join(current, on="symbol", how="left")
        .withColumn("date_key", F.regexp_replace("date", "-", "").cast("int"))
        .select("date_key", "ticker_key", "day_high", "day_low", "avg_close", "total_volume")
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    silver = spark.read.parquet(SILVER_PATH)

    dim_date = build_dim_date(silver)
    dim_ticker = build_dim_ticker(spark, silver)
    fact = build_fact(silver, dim_ticker)

    dim_ticker.orderBy("symbol", "effective_from").show(truncate=False)

    dim_date.write.mode("overwrite").parquet(f"{GOLD}/dim_date")
    dim_ticker.write.mode("overwrite").parquet(f"{GOLD}/dim_ticker")
    fact.write.mode("overwrite").parquet(f"{GOLD}/fact_daily_prices")
    print("wrote dim_date, dim_ticker (SCD2), fact_daily_prices to the gold bucket")


if __name__ == "__main__":
    main()
