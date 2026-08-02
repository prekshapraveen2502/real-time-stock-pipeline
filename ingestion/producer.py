"""Stock data producer.

Fetches the latest 1-minute bar for each configured ticker and publishes it to
Kafka, keyed by symbol, on a fixed interval. Network fetches use bounded retries
with exponential backoff and jitter. Exposes Prometheus metrics and shuts down
cleanly on SIGINT/SIGTERM (so it flushes in-flight messages before exiting).
"""

import json
import logging
import os
import random
import signal
import threading
import time
from datetime import datetime, timezone

import yfinance as yf
from dotenv import load_dotenv
from kafka import KafkaProducer
from prometheus_client import Counter, Histogram, start_http_server

load_dotenv()  # read the .env file into environment variables

# Config from the environment.
SYMBOLS = [s.strip() for s in os.getenv("STOCK_SYMBOLS", "AAPL").split(",") if s.strip()]
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "10"))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock-prices")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("producer")

# Prometheus metrics (scraped at http://localhost:<METRICS_PORT>/metrics).
# "records per minute" is derived on the dashboard as rate(records_sent_total[1m]).
RECORDS_SENT = Counter("stock_records_sent_total", "Records published to Kafka", ["symbol"])
FETCH_RETRIES = Counter("stock_fetch_retries_total", "Fetch attempts that were retried", ["symbol"])
FETCH_FAILURES = Counter("stock_fetch_failures_total", "Fetches that failed after all retries", ["symbol"])
FETCH_LATENCY = Histogram("stock_fetch_latency_seconds", "yfinance fetch latency", ["symbol"])

# Set by the signal handlers so the main loop stops after the current cycle.
_shutdown = threading.Event()


def _handle_signal(signum, _frame):
    logger.info("received signal %s, will stop after the current cycle", signum)
    _shutdown.set()


def _utc(ts):
    """Normalize a pandas timestamp to an ISO-8601 UTC string."""
    return ts.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_latest_bar(symbol):
    """Fetch one ticker's latest 1-minute bar. Raises on failure or empty data."""
    df = yf.Ticker(symbol).history(period="1d", interval="1m", timeout=REQUEST_TIMEOUT)
    if df.empty:
        raise ValueError(f"no data returned for {symbol}")
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


def fetch_with_retry(symbol):
    """Fetch with bounded exponential backoff + jitter. Returns None if all attempts fail."""
    wait = 1
    for attempt in range(1, MAX_RETRIES + 1):
        start = time.perf_counter()
        try:
            bar = fetch_latest_bar(symbol)
            FETCH_LATENCY.labels(symbol=symbol).observe(time.perf_counter() - start)
            return bar
        except Exception as exc:  # noqa: BLE001 - isolate one ticker's failure from the rest
            FETCH_LATENCY.labels(symbol=symbol).observe(time.perf_counter() - start)
            if attempt == MAX_RETRIES:
                FETCH_FAILURES.labels(symbol=symbol).inc()
                logger.error("giving up on %s after %d attempts: %s", symbol, attempt, exc)
                return None
            FETCH_RETRIES.labels(symbol=symbol).inc()
            sleep_for = wait + random.uniform(0, 1)  # exponential backoff + jitter
            logger.warning(
                "fetch %s failed (attempt %d/%d): %s; retrying in %.1fs",
                symbol, attempt, MAX_RETRIES, exc, sleep_for,
            )
            time.sleep(sleep_for)
            wait *= 2
    return None


def build_producer():
    """Create a Kafka producer.

    acks="all" plus retries give durability: the write is acknowledged only after all
    in-sync replicas have it, and transient send failures are retried. Note: the
    kafka-python-ng client has no idempotent-producer mode (enable.idempotence), so a
    retry can create a duplicate. Duplicates are removed downstream by the natural key
    (symbol, timestamp) in the silver job. Switching to confluent-kafka would allow true
    producer idempotence if needed.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
    )


def run():
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    start_http_server(METRICS_PORT)  # expose /metrics
    producer = build_producer()
    logger.info(
        "producer starting: %d symbols -> topic '%s' every %ds (metrics on :%d)",
        len(SYMBOLS), KAFKA_TOPIC, POLL_INTERVAL, METRICS_PORT,
    )
    try:
        while not _shutdown.is_set():
            sent = 0
            for symbol in SYMBOLS:
                record = fetch_with_retry(symbol)
                if record is not None:
                    # key = symbol so all of one ticker's events share a partition (ordering)
                    producer.send(KAFKA_TOPIC, key=symbol, value=record)
                    RECORDS_SENT.labels(symbol=symbol).inc()
                    sent += 1
            producer.flush()  # wait for this batch to be acknowledged
            logger.info("sent %d records to '%s'", sent, KAFKA_TOPIC)
            _shutdown.wait(POLL_INTERVAL)  # interruptible sleep: wakes early on a signal
    finally:
        logger.info("flushing and closing producer")
        producer.flush()
        producer.close()
        logger.info("producer stopped")


if __name__ == "__main__":
    run()
