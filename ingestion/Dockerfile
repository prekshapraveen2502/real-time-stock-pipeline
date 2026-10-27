# Producer image: polls yfinance and publishes to Kafka. Build from the repo root:
#   docker build -f ingestion/Dockerfile -t stock-producer .
FROM python:3.12-slim

# Print logs straight away instead of buffering them, so `docker logs` is live.
ENV PYTHONUNBUFFERED=1
WORKDIR /app

# Install dependencies before copying the code: this layer is cached and only rebuilds when
# requirements.txt changes, not on every code edit.
COPY ingestion/requirements.txt ingestion/requirements.txt
RUN pip install --no-cache-dir -r ingestion/requirements.txt

COPY ingestion/ ingestion/

# Run as an unprivileged user rather than root. yfinance keeps a small cache in $HOME.
RUN useradd --create-home producer
USER producer

# Prometheus /metrics endpoint.
EXPOSE 8000

# Exec form makes python PID 1, so `docker stop` (SIGTERM) reaches the producer's signal handler
# and it flushes Kafka before exiting.
CMD ["python", "ingestion/producer.py"]
