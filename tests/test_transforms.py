"""Unit tests for the Spark transform logic, run on tiny in-memory DataFrames."""

from datetime import date

from processing.silver_job import clean
from processing.gold_job import build_dim_date, build_fact, _incoming_ticker_attrs, _merge_dim_ticker


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


def test_gold_dim_ticker_first_load_starts_at_low_date(spark):
    incoming = spark.createDataFrame(
        [("AAPL", "Apple Inc.", "USD", "yfinance")],
        "symbol string, company_name string, currency string, source string",
    )
    row = _merge_dim_ticker(spark, None, incoming, "2026-08-13").collect()[0]
    assert str(row.effective_from) == "1900-01-01"   # first version is open-ended in the past
    assert str(row.effective_to) == "9999-12-31"
    assert row.is_current is True


def test_gold_first_version_covers_earlier_facts(spark):
    # The fact is dated before the first load's batch date, like the real 2026-07-30 data.
    silver = spark.createDataFrame(
        [("AAPL", "2026-07-30", 334.0, 333.04, 333.85, 882500, "USD", "yfinance")],
        "symbol string, date string, high double, low double, close double, volume long,"
        " currency string, source string",
    )
    dim = _merge_dim_ticker(spark, None, _incoming_ticker_attrs(silver), "2026-08-13")
    row = build_fact(silver, dim).collect()[0]
    assert row.date_key == 20260730
    assert row.ticker_key == 1   # matched the first version instead of NULL


def test_gold_dim_ticker_scd2_boundaries_use_batch_date(spark):
    # An existing current AAPL version sourced from yfinance...
    existing = spark.createDataFrame(
        [(1, "AAPL", "Apple Inc.", "USD", "yfinance", date(2026, 1, 1), date(9999, 12, 31), True)],
        "ticker_key int, symbol string, company_name string, currency string, source string,"
        " effective_from date, effective_to date, is_current boolean",
    )
    # ...whose source now changes, so a new version must be created on the batch date.
    incoming = spark.createDataFrame(
        [("AAPL", "Apple Inc.", "USD", "provider_B")],
        "symbol string, company_name string, currency string, source string",
    )
    merged = _merge_dim_ticker(spark, existing, incoming, "2026-08-13").collect()
    expired = [r for r in merged if not r.is_current][0]
    current = [r for r in merged if r.is_current][0]
    assert str(expired.effective_to) == "2026-08-13"    # old version closed on the batch date
    assert str(current.effective_from) == "2026-08-13"  # new version opens on the batch date
    assert str(current.effective_to) == "9999-12-31"
    assert current.source == "provider_B"
    assert current.ticker_key != expired.ticker_key     # fresh surrogate key


TICKER_ATTRS_SCHEMA = "symbol string, company_name string, currency string, source string"
DIM_TICKER_DDL = (
    "ticker_key int, symbol string, company_name string, currency string, source string,"
    " effective_from date, effective_to date, is_current boolean"
)


def test_gold_dim_ticker_unmapped_symbol_is_stable_across_runs(spark):
    # TSLA is not in COMPANY_NAMES, so company_name is null. Identical reruns must not
    # create a new version each time.
    incoming = spark.createDataFrame([("TSLA", None, "USD", "yfinance")], TICKER_ATTRS_SCHEMA)
    dim = _merge_dim_ticker(spark, None, incoming, "2026-08-13")
    for day in ["2026-08-14", "2026-08-15"]:
        dim = _merge_dim_ticker(spark, spark.createDataFrame(dim.collect(), DIM_TICKER_DDL), incoming, day)
    rows = dim.collect()
    assert len(rows) == 1
    assert rows[0].is_current is True


def test_gold_dim_ticker_new_symbol_later_starts_at_low_date(spark):
    existing = spark.createDataFrame(
        [(1, "AAPL", "Apple Inc.", "USD", "yfinance", date(1900, 1, 1), date(9999, 12, 31), True)],
        DIM_TICKER_DDL,
    )
    incoming = spark.createDataFrame(
        [("AAPL", "Apple Inc.", "USD", "yfinance"), ("MSFT", "Microsoft Corporation", "USD", "yfinance")],
        TICKER_ATTRS_SCHEMA,
    )
    merged = {r.symbol: r for r in _merge_dim_ticker(spark, existing, incoming, "2026-08-13").collect()}
    assert len(merged) == 2                                  # AAPL untouched, MSFT added
    assert merged["AAPL"].ticker_key == 1
    assert str(merged["MSFT"].effective_from) == "1900-01-01"
    assert merged["MSFT"].ticker_key == 2                    # next surrogate key


def test_gold_dim_ticker_null_to_value_is_a_change(spark):
    existing = spark.createDataFrame(
        [(1, "TSLA", None, "USD", "yfinance", date(1900, 1, 1), date(9999, 12, 31), True)],
        DIM_TICKER_DDL,
    )
    incoming = spark.createDataFrame([("TSLA", "Tesla Inc.", "USD", "yfinance")], TICKER_ATTRS_SCHEMA)
    merged = _merge_dim_ticker(spark, existing, incoming, "2026-08-13").collect()
    expired = [r for r in merged if not r.is_current][0]
    current = [r for r in merged if r.is_current][0]
    assert str(expired.effective_to) == "2026-08-13"
    assert current.company_name == "Tesla Inc."
    assert str(current.effective_from) == "2026-08-13"      # a change starts at the batch date


def test_dim_date_attributes(spark):
    silver = spark.createDataFrame([("2026-07-30",)], "date string")
    row = build_dim_date(silver).collect()[0]
    assert row.date_key == 20260730
    assert row.year == 2026
    assert row.month == 7
    assert row.day == 30
    assert row.day_of_week == "Thursday"
    assert row.is_weekend == False
