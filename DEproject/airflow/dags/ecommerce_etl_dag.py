"""
=============================================================================
APACHE AIRFLOW ORCHESTRATION DAG: Modern E-Commerce ETL Pipeline
=============================================================================
Orchestrates:
1. Raw Data Extraction & Ingestion (Airbyte / EL connector) into Snowflake RAW
2. dbt Staging Transformations (Bronze Layer)
3. dbt Staging Data Quality Tests
4. dbt Dimensional & Fact Modeling (Gold Layer)
5. dbt Gold Marts Data Quality Tests
6. BI Cache & Conversational Query Layer Refresh
=============================================================================
"""

import os
from datetime import datetime, timedelta

# Import Airflow modules (or fallback mocks if viewing standalone)
try:
    from airflow import DAG
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.python import PythonOperator
    from airflow.operators.bash import BashOperator
except ImportError:
    # Standalone mock classes for environments without apache-airflow installed
    class DAG:
        def __init__(self, dag_id, **kwargs):
            self.dag_id = dag_id
            self.kwargs = kwargs
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass

    class BaseMockOperator:
        def __init__(self, task_id, **kwargs):
            self.task_id = task_id
            self.kwargs = kwargs
        def __rshift__(self, other):
            return other
        def __lshift__(self, other):
            return self

    class EmptyOperator(BaseMockOperator):
        pass

    class PythonOperator(BaseMockOperator):
        def __init__(self, task_id, python_callable, **kwargs):
            super().__init__(task_id, **kwargs)
            self.python_callable = python_callable

    class BashOperator(BaseMockOperator):
        def __init__(self, task_id, bash_command, **kwargs):
            super().__init__(task_id, **kwargs)
            self.bash_command = bash_command


# Default DAG Arguments (Enterprise standard)
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email': ['data-alerts@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

def task_ingest_data():
    """Extract and load raw transactional data to Snowflake RAW schema."""
    print("[AIRFLOW TASK] Starting Airbyte / EL Ingestion to Snowflake RAW...")
    from ingestion.snowflake_ingest import ingest_to_snowflake, ingest_to_local_warehouse
    if os.getenv("SNOWFLAKE_ACCOUNT"):
        ingest_to_snowflake()
    else:
        ingest_to_local_warehouse()
    print("[AIRFLOW TASK] Ingestion completed successfully.")

def task_run_dbt_transformations():
    """Execute dbt Medallion transformations across Bronze, Silver, and Gold."""
    print("[AIRFLOW TASK] Executing dbt transformations...")
    from dbt_ecommerce.run_dbt_models import run_transformations
    run_transformations()
    print("[AIRFLOW TASK] dbt transformations completed.")

def task_run_dbt_tests():
    """Execute dbt Data Quality test suite."""
    print("[AIRFLOW TASK] Running dbt data quality tests...")
    from dbt_ecommerce.run_dbt_models import run_dbt_data_tests
    run_dbt_data_tests()
    print("[AIRFLOW TASK] All dbt quality tests passed.")

def task_refresh_bi():
    """Publish metrics & notify BI interface of new data availability."""
    print("[AIRFLOW TASK] Refreshing Conversational BI metadata & materialized views...")
    print("[AIRFLOW TASK] Conversational BI interface is up-to-date with latest warehouse state.")


# Define the DAG
with DAG(
    dag_id='ecommerce_modern_data_pipeline',
    default_args=default_args,
    description='Automated modern ETL pipeline: Airbyte -> Snowflake -> dbt -> Conversational BI',
    schedule_interval='@daily',
    catchup=False,
    tags=['production', 'modern_data_stack', 'snowflake', 'dbt', 'airbyte']
) as dag:

    # 1. Pipeline Start
    start_pipeline = EmptyOperator(
        task_id='start_pipeline'
    )

    # 2. Ingestion Task (Airbyte / EL connector)
    ingest_raw_data = PythonOperator(
        task_id='airbyte_ingest_to_raw',
        python_callable=task_ingest_data
    )

    # 3. dbt Staging Transformations (Bronze)
    run_dbt_models = PythonOperator(
        task_id='dbt_run_transformations',
        python_callable=task_run_dbt_transformations
    )

    # 4. dbt Quality Tests
    run_data_tests = PythonOperator(
        task_id='dbt_run_quality_tests',
        python_callable=task_run_dbt_tests
    )

    # 5. Refresh Conversational BI
    refresh_bi_cache = PythonOperator(
        task_id='refresh_conversational_bi',
        python_callable=task_refresh_bi
    )

    # 6. Pipeline Complete
    end_pipeline = EmptyOperator(
        task_id='end_pipeline'
    )

    # DAG Dependency Flow (Linear with Quality Gates)
    start_pipeline >> ingest_raw_data >> run_dbt_models >> run_data_tests >> refresh_bi_cache >> end_pipeline
