"""
Database Connector Module
Provides unified query execution interface for Snowflake and Local Warehouse (DuckDB).
"""

import os
import sys
import pandas as pd

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ingestion.snowflake_ingest import get_snowflake_connection, DUCKDB_AVAILABLE

if DUCKDB_AVAILABLE:
    import duckdb


def is_bigquery_connected():
    """True when the BI app should query BigQuery."""
    from warehouse_config import backend
    return backend() == "bigquery"


def _bigquery_query(query):
    from ingestion.bigquery_ingest import get_bigquery_client
    client = get_bigquery_client()
    df = client.query(query.strip().rstrip(";")).to_dataframe()
    df.columns = [c.lower() for c in df.columns]
    return df


def is_snowflake_connected():
    """Check if Snowflake credentials are configured."""
    return bool(os.getenv("SNOWFLAKE_ACCOUNT") and os.getenv("SNOWFLAKE_USER")
                and (os.getenv("SNOWFLAKE_PASSWORD") or os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")))


def execute_query(query: str, db_path="warehouse.duckdb") -> pd.DataFrame:
    """
    Executes a SQL query against Snowflake (if connected) or Local Warehouse Cache (DuckDB).
    Returns a pandas DataFrame.
    """
    if is_bigquery_connected():
        return _bigquery_query(query)

    if is_snowflake_connected():
        conn = get_snowflake_connection()
        if not conn:
            raise RuntimeError("Snowflake credentials are set but the connection could not be created.")
        try:
            cursor = conn.cursor()
            cursor.execute("USE DATABASE ECOMMERCE_DB")
            cursor.execute(query)
            df = cursor.fetch_pandas_all()
            cursor.close()
            df.columns = [c.lower() for c in df.columns]
            return df
        finally:
            conn.close()

    # Local DuckDB execution
    conn = duckdb.connect(db_path, read_only=True)
    try:
        df = conn.execute(query).df()
        df.columns = [c.lower() for c in df.columns]
        return df
    finally:
        conn.close()


def get_table_counts(db_path="warehouse.duckdb"):
    """Get record counts for all layers."""
    tables = [
        ("raw", "customers"),
        ("raw", "products"),
        ("raw", "orders"),
        ("raw", "order_items"),
        ("raw", "payments"),
        ("staging", "stg_customers"),
        ("staging", "stg_products"),
        ("staging", "stg_orders"),
        ("staging", "int_order_items_detailed"),
        ("staging", "int_customer_orders_summary"),
        ("marts", "dim_customers"),
        ("marts", "dim_products"),
        ("marts", "fact_orders"),
        ("marts", "mart_daily_sales"),
        ("marts", "mart_delivery_sla")
    ]
    counts = {}
    if is_bigquery_connected() or is_snowflake_connected():
        for schema, table in tables:
            try:
                df = execute_query(f"SELECT COUNT(*) AS n FROM {schema}.{table}")
                counts[f"{schema}.{table}"] = int(df.iloc[0, 0])
            except Exception:
                counts[f"{schema}.{table}"] = 0
        return counts

    conn = duckdb.connect(db_path, read_only=True)
    try:
        for schema, table in tables:
            try:
                c = conn.execute(f"SELECT COUNT(*) FROM {schema}.{table}").fetchone()[0]
                counts[f"{schema}.{table}"] = c
            except Exception:
                counts[f"{schema}.{table}"] = 0
    finally:
        conn.close()
    return counts
