"""Stock data producer.

Fetches the latest 1-minute bar for each configured ticker, then repeats on a
fixed interval. Config comes from the environment (12-factor), not hardcoded.
No resilience or Kafka yet -- those come next.
"""

import json
import os
import time
from datetime import datetime, timezone

import yfinance as yf
from dotenv import load_dotenv

load_dotenv()  # read the .env file into environment variables

# Read config from the environment; split symbols on commas and strip spaces.
SYMBOLS = [s.strip() for s in os.getenv("STOCK_SYMBOLS", "AAPL").split(",") if s.strip()]
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))


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


def run():
    while True:  # outer loop: poll forever
        for symbol in SYMBOLS:  # inner loop: every configured ticker
            print(json.dumps(fetch_latest_bar(symbol)))
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    run()
