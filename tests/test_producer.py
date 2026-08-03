"""Unit tests for the producer's record shaping, with yfinance mocked out."""

import pandas as pd

from ingestion import producer


def test_utc_converts_eastern_to_utc():
    ts = pd.Timestamp("2026-07-30 15:59:00", tz="America/New_York")
    assert producer._utc(ts) == "2026-07-30T19:59:00Z"


def test_fetch_latest_bar_shapes_the_contract(monkeypatch):
    index = pd.DatetimeIndex([pd.Timestamp("2026-07-30 15:59:00", tz="America/New_York")])
    bar = pd.DataFrame(
        {"Open": [1.0], "High": [2.0], "Low": [0.5], "Close": [1.5], "Volume": [100]},
        index=index,
    )

    class FakeTicker:
        def __init__(self, symbol):
            pass

        def history(self, **kwargs):
            return bar

    monkeypatch.setattr(producer.yf, "Ticker", FakeTicker)

    record = producer.fetch_latest_bar("AAPL")
    assert record["symbol"] == "AAPL"
    assert record["timestamp"] == "2026-07-30T19:59:00Z"  # normalized to UTC
    assert record["close"] == 1.5
    assert record["volume"] == 100
    assert record["source"] == "yfinance"
    expected_fields = {
        "symbol", "timestamp", "open", "high", "low", "close",
        "volume", "source", "interval", "currency", "ingested_at",
    }
    assert expected_fields <= set(record)
