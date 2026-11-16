"""
Pipeline task functions shared by the Airflow DAG and the local runner.
This module deliberately has no Airflow import so it works anywhere.
"""

import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from warehouse_config import backend  # noqa: E402

DB_PATH = os.getenv("DUCKDB_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "warehouse.duckdb"))

RAW_TABLES = ["raw.customers", "raw.products", "raw.orders", "raw.order_items", "raw.payments"]
MART_TABLES = ["marts.dim_customers", "marts.dim_products", "marts.fact_orders",
               "marts.mart_daily_sales", "marts.mart_delivery_sla"]


def task_ingest_data():
    """EL step: Airbyte sync if configured, else the local CSV loader for the active warehouse."""
    from ingestion.airbyte_trigger import airbyte_configured, trigger_airbyte_sync

    if airbyte_configured():
        print("[ingest] Triggering Airbyte sync...")
        trigger_airbyte_sync()
        return

    if not os.path.exists("data_source/raw_data/orders.csv"):
        from data_source.generate_raw_data import generate_ecommerce_data
        generate_ecommerce_data()

    target = backend()
    print(f"[ingest] AIRBYTE_CONNECTION_ID not set; using the fallback CSV loader -> {target}.")
    if target == "bigquery":
        from ingestion.bigquery_ingest import ingest_to_bigquery
        ingest_to_bigquery()
    elif target == "snowflake":
        from ingestion.snowflake_ingest import ingest_to_snowflake
        ingest_to_snowflake()
    else:
        from ingestion.snowflake_ingest import ingest_to_local_warehouse
        ingest_to_local_warehouse(db_path=DB_PATH)


def task_run_dbt_transformations():
    from dbt_ecommerce.run_dbt_models import run_transformations
    run_transformations()


def task_run_dbt_tests():
    from dbt_ecommerce.run_dbt_models import run_dbt_data_tests
    run_dbt_data_tests()


def task_refresh_bi():
    """Record this run (timestamp + row counts) in marts.pipeline_runs so the BI app can show freshness."""
    counts = {}
    target = backend()

    if target == "bigquery":
        from google.cloud import bigquery
        from ingestion.bigquery_ingest import ensure_dataset, get_bigquery_client
        client = get_bigquery_client()
        ensure_dataset(client, "marts")
        now = datetime.utcnow().isoformat()
        rows = []
        for t in RAW_TABLES + MART_TABLES:
            n = list(client.query(f"SELECT COUNT(*) AS n FROM `{client.project}.{t}`").result())[0].n
            counts[t] = n
            rows.append({"run_at": now, "table_name": t, "row_count": n})
        schema = [bigquery.SchemaField("run_at", "TIMESTAMP"),
                  bigquery.SchemaField("table_name", "STRING"),
                  bigquery.SchemaField("row_count", "INTEGER")]
        # load job (not DML/streaming) so this also works in the free sandbox
        client.load_table_from_json(
            rows, f"{client.project}.marts.pipeline_runs",
            job_config=bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_APPEND")).result()

    elif target == "snowflake":
        from ingestion.snowflake_ingest import get_snowflake_connection
        conn = get_snowflake_connection()
        cur = conn.cursor()
        try:
            cur.execute("USE DATABASE ECOMMERCE_DB")
            cur.execute("CREATE TABLE IF NOT EXISTS MARTS.PIPELINE_RUNS (run_at TIMESTAMP, table_name STRING, row_count NUMBER)")
            now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            for t in RAW_TABLES + MART_TABLES:
                n = cur.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                counts[t] = n
                cur.execute(f"INSERT INTO MARTS.PIPELINE_RUNS VALUES ('{now}', '{t}', {n})")
        finally:
            cur.close()
            conn.close()

    else:
        import duckdb
        conn = duckdb.connect(DB_PATH)
        try:
            conn.execute("CREATE SCHEMA IF NOT EXISTS marts")
            conn.execute("CREATE TABLE IF NOT EXISTS marts.pipeline_runs (run_at TIMESTAMP, table_name VARCHAR, row_count BIGINT)")
            now = datetime.utcnow()
            for t in RAW_TABLES + MART_TABLES:
                n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                counts[t] = n
                conn.execute("INSERT INTO marts.pipeline_runs VALUES (?, ?, ?)", [now, t, n])
        finally:
            conn.close()

    for t, n in counts.items():
        print(f"[refresh_bi] {t}: {n} rows")
