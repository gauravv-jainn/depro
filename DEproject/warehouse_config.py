"""Single place that decides which warehouse the pipeline and BI app talk to."""

import os


def backend():
    """
    Returns "bigquery" or "duckdb".
      1. DBT_TARGET if it is set explicitly
      2. bigquery when GCP_PROJECT is set
      3. otherwise the local DuckDB file
    """
    explicit = os.getenv("DBT_TARGET", "").strip().lower()
    if explicit in ("bigquery", "duckdb"):
        return explicit
    return "bigquery" if os.getenv("GCP_PROJECT") else "duckdb"
