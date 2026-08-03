"""Unit tests for the Spark transform logic, run on tiny in-memory DataFrames."""

from processing.silver_job import clean
from processing.gold_job import build_dim_date, build_fact


def test_silver_clean_dedupes_and_drops_bad_prices(spark):
    df = spark.createDataFrame(
        [
            ("AAPL", "t1", 100.0),
            ("AAPL", "t1", 100.0),  # duplicate natural key -> should collapse to one
            ("MSFT", "t1", 200.0),
            ("AMZN", "t2", -5.0),   # non-positive price -> should be dropped
        ],
        "symbol string, timestamp string, close double",
    )
    result = clean(df)
    assert result.count() == 2
    assert sorted(r.symbol for r in result.collect()) == ["AAPL", "MSFT"]


def test_gold_fact_aggregates_measures(spark):
    silver = spark.createDataFrame(
        [
            ("AAPL", "2026-07-30", 10.0, 8.0, 9.0, 100),
            ("AAPL", "2026-07-30", 12.0, 7.0, 11.0, 50),
        ],
        "symbol string, date string, high double, low double, close double, volume long",
    )
    dim_ticker = spark.createDataFrame(
        [(1, "AAPL", True)],
        "ticker_key int, symbol string, is_current boolean",
    )
    row = build_fact(silver, dim_ticker).collect()[0]
    assert row.date_key == 20260730
    assert row.ticker_key == 1
    assert row.day_high == 12.0
    assert row.day_low == 7.0
    assert row.avg_close == 10.0        # avg(9, 11)
    assert row.total_volume == 150      # 100 + 50


def test_dim_date_attributes(spark):
    silver = spark.createDataFrame([("2026-07-30",)], "date string")
    row = build_dim_date(silver).collect()[0]
    assert row.date_key == 20260730
    assert row.year == 2026
    assert row.month == 7
    assert row.day == 30
    assert row.day_of_week == "Thursday"
    assert row.is_weekend == False
