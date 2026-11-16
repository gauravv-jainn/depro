"""
Local pipeline runner (no Airflow server needed).

Runs the same task functions, in the same order, as the Airflow DAG
(orchestration/dags/ecommerce_etl_dag.py). This is NOT Airflow: use the Docker
setup in orchestration/docker-compose.yaml for the real scheduler and UI.
"""

import os
import sys
import time

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)
os.chdir(ROOT)

from pipeline_tasks import (task_ingest_data, task_refresh_bi,  # noqa: E402
                            task_run_dbt_tests, task_run_dbt_transformations)

TASKS = [
    ("ingest_to_raw", task_ingest_data),
    ("dbt_run", task_run_dbt_transformations),
    ("dbt_test", task_run_dbt_tests),
    ("refresh_conversational_bi", task_refresh_bi),
]


def run_pipeline():
    results = []
    for task_id, fn in TASKS:
        print(f"\n>> {task_id}", flush=True)
        start = time.time()
        try:
            fn()
        except Exception as exc:
            print(f"   [FAILED] {task_id}: {exc}")
            results.append((task_id, "FAILED", round(time.time() - start, 1)))
            break
        results.append((task_id, "SUCCESS", round(time.time() - start, 1)))

    print("\nRun summary")
    for task_id, status, secs in results:
        print(f"  {task_id:<28} {status:<8} {secs}s")
    return all(s == "SUCCESS" for _, s, _ in results) and len(results) == len(TASKS)


if __name__ == "__main__":
    sys.exit(0 if run_pipeline() else 1)
