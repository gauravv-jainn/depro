"""
Fallback CSV loader for the local DuckDB warehouse (used when no cloud warehouse is configured).
"""

import os
import sys

# Ensure root directory is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
from datetime import datetime

try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False


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
    if not os.path.exists("data_source/raw_data/orders.csv"):
        print("[*] Generating raw data first...")
        from data_source.generate_raw_data import generate_ecommerce_data
        generate_ecommerce_data()
    ingest_to_local_warehouse()
