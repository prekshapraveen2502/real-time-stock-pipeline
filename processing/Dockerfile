# Spark image: runs the bronze streaming job and the silver, gold and warehouse batch jobs.
# Build from the repo root:
#   docker build -f processing/Dockerfile -t stock-spark .
FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1

# Spark runs on the JVM. Java 17 is supported by Spark 4.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY processing/requirements.txt processing/requirements.txt
RUN pip install --no-cache-dir -r processing/requirements.txt

# Connector jars, baked in so jobs start without resolving --packages from Maven on every run.
# These are the exact versions Spark resolved for spark-sql-kafka 4.2.0, hadoop-aws 3.5.0 and the
# Postgres JDBC driver; pyspark already ships the matching hadoop-client 3.5.0 jars.
ARG MAVEN=https://repo1.maven.org/maven2
RUN JARS="$(python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')" \
    && cd "$JARS" \
    && for path in \
        org/apache/spark/spark-sql-kafka-0-10_2.13/4.2.0/spark-sql-kafka-0-10_2.13-4.2.0.jar \
        org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.2.0/spark-token-provider-kafka-0-10_2.13-4.2.0.jar \
        org/apache/kafka/kafka-clients/3.9.2/kafka-clients-3.9.2.jar \
        org/apache/commons/commons-pool2/2.13.1/commons-pool2-2.13.1.jar \
        org/apache/hadoop/hadoop-aws/3.5.0/hadoop-aws-3.5.0.jar \
        software/amazon/awssdk/bundle/2.35.4/bundle-2.35.4.jar \
        software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/1.3.1/analyticsaccelerator-s3-1.3.1.jar \
        org/wildfly/openssl/wildfly-openssl/2.2.5.Final/wildfly-openssl-2.2.5.Final.jar \
        org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar \
        org/checkerframework/checker-qual/3.42.0/checker-qual-3.42.0.jar \
      ; do curl -fsSLO "$MAVEN/$path" || exit 1; done

COPY processing/ processing/
COPY warehouse/ warehouse/

# Run as an unprivileged user. The streaming checkpoint directory is mounted at
# /app/spark-checkpoints by docker compose.
RUN useradd --create-home spark && mkdir -p /app/spark-checkpoints && chown spark /app/spark-checkpoints
USER spark

CMD ["python", "processing/streaming_job.py"]
