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


_BQ_CLIENT = None


def _bq_client():
    """One BigQuery client per process (creating it per query is slow)."""
    global _BQ_CLIENT
    if _BQ_CLIENT is None:
        from ingestion.bigquery_ingest import get_bigquery_client
        _BQ_CLIENT = get_bigquery_client()
    return _BQ_CLIENT


def _bigquery_query(query):
    client = _bq_client()
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
        # one query for all tables (a query per table made every page load take ~20 s)
        union = " UNION ALL ".join(
            f"SELECT '{s}.{t}' AS t, COUNT(*) AS n FROM {s}.{t}" for s, t in tables)
        try:
            df = execute_query(union)
            return {r.t: int(r.n) for r in df.itertuples()}
        except Exception:
            pass  # a table is missing: fall back to one query per table
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


def describe_table(qualified_name: str) -> pd.DataFrame:
    """Column names and types of schema.table (DuckDB DESCRIBE has no BigQuery equivalent)."""
    if is_bigquery_connected():
        schema, table = qualified_name.split(".", 1)
        return execute_query(
            f"SELECT column_name, data_type FROM {schema}.INFORMATION_SCHEMA.COLUMNS "
            f"WHERE table_name = '{table}' ORDER BY ordinal_position")
    df = execute_query(f"DESCRIBE {qualified_name}")
    return df[["column_name", "column_type"]].rename(columns={"column_type": "data_type"})
