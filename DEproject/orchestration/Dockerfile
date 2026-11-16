# Airflow image with dbt in an isolated venv (avoids dependency clashes with Airflow's pinned packages)
FROM apache/airflow:2.9.2-python3.11

USER airflow

# dbt lives in its own virtualenv; the DAG calls /home/airflow/dbt_venv/bin/dbt
RUN python -m venv /home/airflow/dbt_venv \
 && /home/airflow/dbt_venv/bin/pip install --no-cache-dir \
      "dbt-core~=1.8.0" "dbt-duckdb~=1.8.0" "dbt-snowflake~=1.8.0" "dbt-bigquery~=1.8.0" "duckdb<1.2"

# Libraries used by pipeline_tasks.py (runs inside Airflow's own Python)
RUN pip install --no-cache-dir "duckdb<1.2" "snowflake-connector-python[pandas]" google-cloud-bigquery db-dtypes pyarrow requests
