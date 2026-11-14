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


def is_snowflake_connected():
    """Check if Snowflake credentials are configured."""
    return bool(os.getenv("SNOWFLAKE_ACCOUNT") and os.getenv("SNOWFLAKE_USER") and os.getenv("SNOWFLAKE_PASSWORD"))


def execute_query(query: str, db_path="warehouse.duckdb") -> pd.DataFrame:
    """
    Executes a SQL query against Snowflake (if connected) or Local Warehouse Cache (DuckDB).
    Returns a pandas DataFrame.
    """
    if is_snowflake_connected():
        try:
            conn = get_snowflake_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute("USE DATABASE ECOMMERCE_DB")
                cursor.execute(query)
                df = cursor.fetch_pandas_all()
                cursor.close()
                conn.close()
                # Normalize column names to lowercase
                df.columns = [c.lower() for c in df.columns]
                return df
        except Exception as e:
            print(f"[!] Snowflake query error: {e}. Falling back to local warehouse.")

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
