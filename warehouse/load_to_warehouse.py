"""Load the Gold star schema from MinIO into the Postgres warehouse.

Reads each Gold table (dim_date, dim_ticker, fact_daily_prices) from the gold bucket
and writes it to a Postgres table of the same name. Postgres is our local stand-in for
Snowflake; the shape of the job would be the same either way.
"""

import os

from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")

GOLD = "s3a://gold"
TABLES = ["dim_date", "dim_ticker", "fact_daily_prices"]

PG_HOST = os.getenv("WAREHOUSE_HOST", "localhost")
PG_PORT = os.getenv("WAREHOUSE_PORT", "5432")
PG_DB = os.getenv("WAREHOUSE_DB", "stock_warehouse")
PG_USER = os.getenv("WAREHOUSE_USER", "warehouse")
PG_PASSWORD = os.getenv("WAREHOUSE_PASSWORD", "warehouse123")
JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"


def build_spark():
    return (
        SparkSession.builder
        .appName("load-star-schema-to-warehouse")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def load_table(spark, name):
    df = spark.read.parquet(f"{GOLD}/{name}")
    (
        df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", name)
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )
    print(f"loaded {name}: {df.count()} rows")


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    for name in TABLES:
        load_table(spark, name)


if __name__ == "__main__":
    main()
