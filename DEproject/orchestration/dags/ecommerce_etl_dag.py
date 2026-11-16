"""
Apache Airflow DAG: ecommerce_modern_data_pipeline

  start >> ingest (Airbyte sync or local loader) >> dbt run >> dbt test >> refresh_bi >> end

dbt is executed with the real dbt CLI via BashOperator. A failing `dbt test`
blocks the BI refresh, so bad data never reaches the dashboard.
Requires apache-airflow and dbt (see orchestration/docker-compose.yaml).
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from pipeline_tasks import task_ingest_data, task_refresh_bi

DBT_DIR = os.getenv("DBT_DIR", "/opt/airflow/dbt_ecommerce")
DBT_FLAGS = f"--project-dir {DBT_DIR} --profiles-dir {DBT_DIR}"

default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ecommerce_modern_data_pipeline",
    default_args=default_args,
    description="Airbyte -> Snowflake/DuckDB -> dbt -> Conversational BI",
    schedule="@daily",
    catchup=False,
    tags=["modern_data_stack", "dbt", "airbyte"],
) as dag:

    start_pipeline = EmptyOperator(task_id="start_pipeline")

    ingest_raw_data = PythonOperator(task_id="ingest_to_raw", python_callable=task_ingest_data)

    dbt_run = BashOperator(task_id="dbt_run", bash_command=f"dbt run {DBT_FLAGS}")

    dbt_test = BashOperator(task_id="dbt_test", bash_command=f"dbt test {DBT_FLAGS}")

    refresh_bi = PythonOperator(task_id="refresh_conversational_bi", python_callable=task_refresh_bi)

    end_pipeline = EmptyOperator(task_id="end_pipeline")

    start_pipeline >> ingest_raw_data >> dbt_run >> dbt_test >> refresh_bi >> end_pipeline
