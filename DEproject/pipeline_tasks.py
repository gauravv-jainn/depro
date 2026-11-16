"""
Pipeline task functions shared by the Airflow DAG and the local runner.
This module deliberately has no Airflow import so it works anywhere.
"""

import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

DB_PATH = os.getenv("DUCKDB_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "warehouse.duckdb"))

RAW_TABLES = ["raw.customers", "raw.products", "raw.orders", "raw.order_items", "raw.payments"]
MART_TABLES = ["marts.dim_customers", "marts.dim_products", "marts.fact_orders",
               "marts.mart_daily_sales", "marts.mart_delivery_sla"]


def use_snowflake():
    return bool(os.getenv("SNOWFLAKE_ACCOUNT")) and os.getenv("DBT_TARGET", "snowflake") == "snowflake"


def task_ingest_data():
    """EL step: Airbyte sync if configured, else the local CSV loader."""
    from ingestion.airbyte_trigger import airbyte_configured, trigger_airbyte_sync
    from ingestion.snowflake_ingest import ingest_to_local_warehouse, ingest_to_snowflake

    if airbyte_configured():
        print("[ingest] Triggering Airbyte sync...")
        trigger_airbyte_sync()
        return

    if not os.path.exists("data_source/raw_data/orders.csv"):
        from data_source.generate_raw_data import generate_ecommerce_data
        generate_ecommerce_data()
    if use_snowflake():
        print("[ingest] AIRBYTE_CONNECTION_ID not set; loading CSVs into Snowflake RAW directly.")
        ingest_to_snowflake()
    else:
        print("[ingest] AIRBYTE_CONNECTION_ID not set; loading CSVs into local DuckDB RAW.")
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
    if use_snowflake():
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
