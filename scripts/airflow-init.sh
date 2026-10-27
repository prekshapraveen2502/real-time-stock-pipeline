#!/usr/bin/env bash
# One-time Airflow setup, run by the airflow-init container before the webserver and scheduler start.
# Safe to run again: every step checks before it creates anything.
set -euo pipefail

# 1. Make sure Airflow's own metadata database exists in the shared Postgres server.
python - <<'EOF'
import os
import psycopg2

conn = psycopg2.connect(
    host=os.environ["WAREHOUSE_HOST"],
    port=os.environ["WAREHOUSE_PORT"],
    dbname=os.environ["WAREHOUSE_DB"],
    user=os.environ["WAREHOUSE_USER"],
    password=os.environ["WAREHOUSE_PASSWORD"],
)
conn.autocommit = True  # CREATE DATABASE cannot run inside a transaction
cur = conn.cursor()
cur.execute("SELECT 1 FROM pg_database WHERE datname = 'airflow'")
if cur.fetchone() is None:
    cur.execute("CREATE DATABASE airflow")
    print("created database airflow")
else:
    print("database airflow already exists")
EOF

# 2. Create or upgrade Airflow's tables.
airflow db migrate

# 3. Create the admin login once.
if airflow users list --output plain | awk '{print $2}' | grep -qx "${AIRFLOW_ADMIN_USER}"; then
    echo "admin user ${AIRFLOW_ADMIN_USER} already exists"
else
    airflow users create \
        --role Admin \
        --username "${AIRFLOW_ADMIN_USER}" \
        --password "${AIRFLOW_ADMIN_PASSWORD}" \
        --firstname Admin --lastname User --email admin@example.com
fi
