"""Stock data producer.

Fetches the latest 1-minute bar for each configured ticker. Config comes from
the environment (12-factor), not hardcoded. No loop, resilience, or Kafka yet --
those come next.
"""

import json
import os
from datetime import datetime, timezone

import yfinance as yf
from dotenv import load_dotenv

load_dotenv()  # read the .env file into environment variables

# Read the ticker list from config; split on commas and strip stray spaces.
SYMBOLS = [s.strip() for s in os.getenv("STOCK_SYMBOLS", "AAPL").split(",") if s.strip()]


def _utc(ts):
    """Normalize a pandas timestamp to an ISO-8601 UTC string."""
    return ts.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_latest_bar(symbol):
    df = yf.Ticker(symbol).history(period="1d", interval="1m")
    latest = df.iloc[-1]
    return {
        "symbol": symbol,
        "timestamp": _utc(latest.name),
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
    for symbol in SYMBOLS:
        print(json.dumps(fetch_latest_bar(symbol)))
