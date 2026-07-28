"""Minimal first step: fetch one stock's latest 1-minute bar and print it.

No Kafka, no resilience yet. The only goal is to prove we can pull real data
from yfinance and shape it into our data contract, with all timestamps in UTC.
"""

from datetime import datetime, timezone

import yfinance as yf

SYMBOL = "AAPL"


def _utc(ts):
    """Normalize a pandas timestamp to an ISO-8601 UTC string like 2026-07-28T19:59:00Z."""
    return ts.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_latest_bar(symbol):
    ticker = yf.Ticker(symbol)
    # period="1d", interval="1m" -> the most recent trading day, minute by minute
    df = ticker.history(period="1d", interval="1m")
    latest = df.iloc[-1]  # the last row = the most recent 1-minute bar

    return {
        "symbol": symbol,
        "timestamp": _utc(latest.name),  # event time, normalized to UTC at ingestion
        "open": round(float(latest["Open"]), 2),
        "high": round(float(latest["High"]), 2),
        "low": round(float(latest["Low"]), 2),
        "close": round(float(latest["Close"]), 2),
        "volume": int(latest["Volume"]),
        "source": "yfinance",
        "interval": "1m",
        "currency": "USD",
        "ingested_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


if __name__ == "__main__":
    import json
    print(json.dumps(fetch_latest_bar(SYMBOL), indent=2))
