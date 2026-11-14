"""
Data Ingestion Service (EL Layer - Airbyte style)
Ingests raw transactional e-commerce CSV data into the Cloud Data Warehouse (Snowflake)
or local warehouse cache.
"""

import os
import sys

# Ensure root directory is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import glob
import pandas as pd
from datetime import datetime

# Optional Snowflake connector import
try:
    import snowflake.connector
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False

try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False


def get_snowflake_connection():
    """Connect to Snowflake using environment variables."""
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "ECOMMERCE_WH")
    database = os.getenv("SNOWFLAKE_DATABASE", "ECOMMERCE_DB")
    schema = os.getenv("SNOWFLAKE_SCHEMA", "RAW")
    role = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")

    if not all([account, user, password]):
        return None

    return snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
        database=database,
        schema=schema,
        role=role
    )


def ingest_to_snowflake(data_dir="data_source/raw_data"):
    """Ingest CSV files into Snowflake RAW tables."""
    conn = get_snowflake_connection()
    if not conn:
        print("[!] Snowflake credentials not set or incomplete in environment.")
        print("[!] Defaulting to local warehouse cache for local development/testing.")
        return ingest_to_local_warehouse(data_dir)

    cursor = conn.cursor()
    print("[*] Connected to Snowflake: ECOMMERCE_DB.RAW")

    table_mappings = {
        "customers.csv": "CUSTOMERS",
        "products.csv": "PRODUCTS",
        "orders.csv": "ORDERS",
        "order_items.csv": "ORDER_ITEMS",
        "payments.csv": "PAYMENTS"
    }

    try:
        cursor.execute("USE DATABASE ECOMMERCE_DB")
        cursor.execute("USE SCHEMA RAW")

        for csv_file, table_name in table_mappings.items():
            file_path = os.path.join(data_dir, csv_file)
            if not os.path.exists(file_path):
                print(f"[!] Warning: {file_path} not found.")
                continue

            df = pd.read_csv(file_path)
            df["_INGESTED_AT"] = datetime.now()

            # Truncate and write via pandas Snowflake engine or staging table
            print(f"[*] Ingesting {len(df)} rows into RAW.{table_name}...")
            cursor.execute(f"TRUNCATE TABLE IF EXISTS RAW.{table_name}")
            
            # Simple bulk insert or stage copy
            from snowflake.connector.pandas_tools import write_pandas
            success, nchunks, nrows, _ = write_pandas(conn, df, table_name, schema="RAW")
            print(f"[+] Loaded {nrows} rows into RAW.{table_name} (Success: {success})")

        print("[OK] All raw tables ingested into Snowflake successfully.")
    finally:
        cursor.close()
        conn.close()


def ingest_to_local_warehouse(data_dir="data_source/raw_data", db_path="warehouse.duckdb"):
    """Ingest raw CSV files into local warehouse cache (DuckDB) for offline/testing mode."""
    if not DUCKDB_AVAILABLE:
        print("[!] duckdb package not yet installed. Please wait for install to complete.")
        return False

    print(f"[*] Initializing local warehouse cache: {db_path}...")
    conn = duckdb.connect(db_path)
    
    # Create schemas
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    conn.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    conn.execute("CREATE SCHEMA IF NOT EXISTS marts;")

    table_mappings = {
        "customers.csv": "customers",
        "products.csv": "products",
        "orders.csv": "orders",
        "order_items.csv": "order_items",
        "payments.csv": "payments"
    }

    for csv_file, table_name in table_mappings.items():
        file_path = os.path.join(data_dir, csv_file)
        if not os.path.exists(file_path):
            print(f"[!] File {file_path} not found.")
            continue
        
        df = pd.read_csv(file_path)
        df["_ingested_at"] = datetime.now().isoformat()
        
        # Load directly into duckdb raw schema
        conn.execute(f"CREATE OR REPLACE TABLE raw.{table_name} AS SELECT * FROM df;")
        count = conn.execute(f"SELECT COUNT(*) FROM raw.{table_name}").fetchall()[0][0]
        print(f"[+] Ingested raw.{table_name} ({count} rows)")

    conn.close()
    print("[SUCCESS] Local warehouse ingestion complete!")
    return True


if __name__ == "__main__":
    # Check if raw data exists; if not, generate it first
    if not os.path.exists("data_source/raw_data/orders.csv"):
        print("[*] Generating raw data first...")
        from data_source.generate_raw_data import generate_ecommerce_data
        generate_ecommerce_data()

    if os.getenv("SNOWFLAKE_ACCOUNT"):
        ingest_to_snowflake()
    else:
        ingest_to_local_warehouse()
