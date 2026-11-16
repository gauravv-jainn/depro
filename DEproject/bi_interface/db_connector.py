"""
Database Connector Module
Unified query interface for BigQuery (when GCP_PROJECT is set) and the local DuckDB warehouse.
"""

import os
import sys
import pandas as pd

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ingestion.local_ingest import DUCKDB_AVAILABLE

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


def execute_query(query: str, db_path="warehouse.duckdb") -> pd.DataFrame:
    """
    Runs a SQL query on BigQuery (if GCP_PROJECT is set) or the local DuckDB warehouse.
    Returns a pandas DataFrame with lower-case column names.
    """
    if is_bigquery_connected():
        return _bigquery_query(query)

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
    if is_bigquery_connected():
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
                counts[f"{schema}.{table}"] = conn.execute(f"SELECT COUNT(*) FROM {schema}.{table}").fetchone()[0]
            except Exception:
                counts[f"{schema}.{table}"] = 0
    finally:
        conn.close()
    return counts
