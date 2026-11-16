"""
Trigger a real Airbyte sync (OSS API) and wait for it to finish.

Used by the Airflow ingestion task when AIRBYTE_CONNECTION_ID is configured.
Airbyte moves data from the source into the warehouse RAW schema; this module
only orchestrates it (the same thing Airflow's Airbyte operator does).
"""

import os
import time

import requests


def _auth():
    user, pwd = os.getenv("AIRBYTE_USERNAME"), os.getenv("AIRBYTE_PASSWORD")
    return (user, pwd) if user and pwd else None


def trigger_airbyte_sync(timeout_s=1800, poll_s=10):
    base = os.getenv("AIRBYTE_URL", "http://localhost:8000").rstrip("/")
    connection_id = os.environ["AIRBYTE_CONNECTION_ID"]

    resp = requests.post(f"{base}/api/v1/connections/sync",
                         json={"connectionId": connection_id}, auth=_auth(), timeout=30)
    resp.raise_for_status()
    job_id = resp.json()["job"]["id"]
    print(f"[airbyte] sync job {job_id} started for connection {connection_id}")

    deadline = time.time() + timeout_s
    while time.time() < deadline:
        job = requests.post(f"{base}/api/v1/jobs/get", json={"id": job_id},
                            auth=_auth(), timeout=30).json()["job"]
        status = job["status"]
        print(f"[airbyte] job {job_id}: {status}")
        if status == "succeeded":
            return job_id
        if status in ("failed", "cancelled", "incomplete"):
            raise RuntimeError(f"Airbyte job {job_id} ended with status '{status}'")
        time.sleep(poll_s)
    raise TimeoutError(f"Airbyte job {job_id} did not finish within {timeout_s}s")


def airbyte_configured():
    return bool(os.getenv("AIRBYTE_CONNECTION_ID"))
