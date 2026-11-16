"""
Fallback CSV loader for BigQuery (used when no Airbyte connection is configured).

Loads the raw CSVs into <GCP_PROJECT>.raw.<table> with load jobs (works in the free
BigQuery sandbox, which does not allow DML or streaming inserts).
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd

TABLES = {
    "customers.csv": ("customers", ["signup_date"]),
    "products.csv": ("products", []),
    "orders.csv": ("orders", ["order_date", "estimated_delivery_date", "actual_delivery_date"]),
    "order_items.csv": ("order_items", []),
    "payments.csv": ("payments", ["payment_date"]),
}


def get_bigquery_client():
    from google.cloud import bigquery
    project = os.environ["GCP_PROJECT"]
    return bigquery.Client(project=project, location=os.getenv("BQ_LOCATION", "US"))


def ensure_dataset(client, name):
    from google.cloud import bigquery
    ds = bigquery.Dataset(f"{client.project}.{name}")
    ds.location = os.getenv("BQ_LOCATION", "US")
    client.create_dataset(ds, exists_ok=True)


def ingest_to_bigquery(data_dir="data_source/raw_data"):
    from google.cloud import bigquery

    client = get_bigquery_client()
    ensure_dataset(client, "raw")
    print(f"[*] Loading CSVs into BigQuery {client.project}.raw ...")

    for csv_file, (table, ts_cols) in TABLES.items():
        path = os.path.join(data_dir, csv_file)
        if not os.path.exists(path):
            print(f"[!] {path} not found, skipping")
            continue
        df = pd.read_csv(path)
        for col in ts_cols:
            df[col] = pd.to_datetime(df[col], errors="coerce")
        df["_ingested_at"] = pd.Timestamp.now()

        job = client.load_table_from_dataframe(
            df, f"{client.project}.raw.{table}",
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
        job.result()
        print(f"[+] raw.{table}: {len(df)} rows")
    print("[OK] BigQuery raw layer loaded.")


if __name__ == "__main__":
    ingest_to_bigquery()
