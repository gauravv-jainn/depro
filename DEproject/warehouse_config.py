"""Single place that decides which warehouse the pipeline and BI app talk to."""

import os


def backend():
    """
    Returns "bigquery", "snowflake" or "duckdb".
      1. DBT_TARGET if it is set explicitly
      2. bigquery when GCP_PROJECT is set
      3. snowflake when SNOWFLAKE_ACCOUNT is set
      4. otherwise the local DuckDB file
    """
    explicit = os.getenv("DBT_TARGET", "").strip().lower()
    if explicit in ("bigquery", "snowflake", "duckdb"):
        return explicit
    if os.getenv("GCP_PROJECT"):
        return "bigquery"
    if os.getenv("SNOWFLAKE_ACCOUNT"):
        return "snowflake"
    return "duckdb"
