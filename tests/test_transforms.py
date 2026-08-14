"""Unit tests for the Spark transform logic, run on tiny in-memory DataFrames."""

from datetime import date

from processing.silver_job import clean
from processing.gold_job import build_dim_date, build_fact


def test_silver_clean_keeps_latest_and_drops_bad_prices(spark):
    df = spark.createDataFrame(
        [
            # same natural key (AAPL, t1): keep the later-arriving version (205.0)
            ("AAPL", "t1", 200.0, "2026-08-13 10:01:00", 1),
            ("AAPL", "t1", 205.0, "2026-08-13 10:03:00", 2),
            ("MSFT", "t1", 300.0, "2026-08-13 10:00:00", 3),
            ("AMZN", "t2", -5.0, "2026-08-13 10:00:00", 4),  # non-positive -> dropped
        ],
        "symbol string, timestamp string, close double, bronze_ingested_at string, kafka_offset long",
    )
    result = clean(df)
    kept = {r.symbol: r.close for r in result.collect()}
    assert result.count() == 2
    assert kept["AAPL"] == 205.0   # deterministically kept the latest bronze_ingested_at
    assert "MSFT" in kept
    assert "AMZN" not in kept      # close <= 0 dropped


def test_gold_fact_aggregates_measures(spark):
    silver = spark.createDataFrame(
        [
            ("AAPL", "2026-07-30", 10.0, 8.0, 9.0, 100),
            ("AAPL", "2026-07-30", 12.0, 7.0, 11.0, 50),
        ],
        "symbol string, date string, high double, low double, close double, volume long",
    )
    dim_ticker = spark.createDataFrame(
        [(1, "AAPL", "Apple Inc.", "USD", "yfinance", date(2026, 1, 1), date(9999, 12, 31), True)],
        "ticker_key int, symbol string, company_name string, currency string, source string,"
        " effective_from date, effective_to date, is_current boolean",
    )
    row = build_fact(silver, dim_ticker).collect()[0]
    assert row.date_key == 20260730
    assert row.ticker_key == 1
    assert row.day_high == 12.0
    assert row.day_low == 7.0
    assert row.avg_close == 10.0        # avg(9, 11)
    assert row.total_volume == 150      # 100 + 50


def test_gold_fact_matches_historical_ticker_version(spark):
    silver = spark.createDataFrame(
        [
            ("AAPL", "2026-03-15", 10.0, 8.0, 9.0, 100),   # historical -> old version
            ("AAPL", "2026-09-15", 12.0, 7.0, 11.0, 50),   # later -> new version
            ("AAPL", "2026-08-01", 5.0, 4.0, 4.5, 10),     # boundary: effective_to is exclusive
        ],
        "symbol string, date string, high double, low double, close double, volume long",
    )
    dim_ticker = spark.createDataFrame(
        [
            (1, "AAPL", "Apple Inc.", "USD", "yfinance", date(2026, 1, 1), date(2026, 8, 1), False),
            (7, "AAPL", "Apple Inc.", "USD", "provider_B", date(2026, 8, 1), date(9999, 12, 31), True),
        ],
        "ticker_key int, symbol string, company_name string, currency string, source string,"
        " effective_from date, effective_to date, is_current boolean",
    )
    got = {r.date_key: r.ticker_key for r in build_fact(silver, dim_ticker).collect()}
    assert got[20260315] == 1   # historical fact -> old version
    assert got[20260915] == 7   # later fact -> new (current) version
    assert got[20260801] == 7   # boundary: effective_from inclusive, effective_to exclusive


def test_dim_date_attributes(spark):
    silver = spark.createDataFrame([("2026-07-30",)], "date string")
    row = build_dim_date(silver).collect()[0]
    assert row.date_key == 20260730
    assert row.year == 2026
    assert row.month == 7
    assert row.day == 30
    assert row.day_of_week == "Thursday"
    assert row.is_weekend == False
