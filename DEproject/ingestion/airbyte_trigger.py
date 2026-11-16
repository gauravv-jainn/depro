"""
Trigger real Airbyte syncs and wait for them to finish.

Used by the Airflow ingestion task when AIRBYTE_CONNECTION_ID is configured.
Airbyte moves data from the sources into the warehouse RAW schema; this module
only orchestrates it (the same thing Airflow's Airbyte operator does).

Env vars:
  AIRBYTE_URL             e.g. http://localhost:8000
  AIRBYTE_CONNECTION_ID   one connection id, or several separated by commas
  AIRBYTE_CLIENT_ID / AIRBYTE_CLIENT_SECRET
                          Airbyte application credentials (abctl: `abctl local credentials`)
                          -> uses the public API with a bearer token (Airbyte 1.x)
  AIRBYTE_USERNAME / AIRBYTE_PASSWORD
                          fallback: basic auth against the legacy config API
"""

import os
import time

import requests


def _base():
    return os.getenv("AIRBYTE_URL", "http://localhost:8000").rstrip("/")


def _bearer_headers():
    """Get a short-lived access token with the application client credentials."""
    resp = requests.post(
        f"{_base()}/api/public/v1/applications/token",
        json={"client_id": os.environ["AIRBYTE_CLIENT_ID"],
              "client_secret": os.environ["AIRBYTE_CLIENT_SECRET"],
              "grant-type": "client_credentials"},
        timeout=30)
    resp.raise_for_status()
    return {"Authorization": f"Bearer {resp.json()['access_token']}"}


def _sync_public_api(connection_id, timeout_s, poll_s):
    headers = _bearer_headers()
    resp = requests.post(f"{_base()}/api/public/v1/jobs",
                         json={"connectionId": connection_id, "jobType": "sync"},
                         headers=headers, timeout=30)
    resp.raise_for_status()
    job_id = resp.json()["jobId"]
    print(f"[airbyte] connection {connection_id}: job {job_id} started")

    deadline = time.time() + timeout_s
    while time.time() < deadline:
        status = requests.get(f"{_base()}/api/public/v1/jobs/{job_id}",
                              headers=headers, timeout=30).json()["status"]
        print(f"[airbyte] job {job_id}: {status}")
        if status == "succeeded":
            return job_id
        if status in ("failed", "cancelled", "incomplete"):
            raise RuntimeError(f"Airbyte job {job_id} ended with status '{status}'")
        time.sleep(poll_s)
    raise TimeoutError(f"Airbyte job {job_id} did not finish within {timeout_s}s")


def _sync_legacy_api(connection_id, timeout_s, poll_s):
    auth = (os.getenv("AIRBYTE_USERNAME"), os.getenv("AIRBYTE_PASSWORD"))
    auth = auth if all(auth) else None
    resp = requests.post(f"{_base()}/api/v1/connections/sync",
                         json={"connectionId": connection_id}, auth=auth, timeout=30)
    resp.raise_for_status()
    job_id = resp.json()["job"]["id"]
    print(f"[airbyte] connection {connection_id}: job {job_id} started")

    deadline = time.time() + timeout_s
    while time.time() < deadline:
        job = requests.post(f"{_base()}/api/v1/jobs/get", json={"id": job_id},
                            auth=auth, timeout=30).json()["job"]
        print(f"[airbyte] job {job_id}: {job['status']}")
        if job["status"] == "succeeded":
            return job_id
        if job["status"] in ("failed", "cancelled", "incomplete"):
            raise RuntimeError(f"Airbyte job {job_id} ended with status '{job['status']}'")
        time.sleep(poll_s)
    raise TimeoutError(f"Airbyte job {job_id} did not finish within {timeout_s}s")


def trigger_airbyte_sync(timeout_s=1800, poll_s=10):
    """Run a sync for every connection in AIRBYTE_CONNECTION_ID (comma separated)."""
    ids = [c.strip() for c in os.environ["AIRBYTE_CONNECTION_ID"].split(",") if c.strip()]
    sync = _sync_public_api if os.getenv("AIRBYTE_CLIENT_ID") else _sync_legacy_api
    return [sync(cid, timeout_s, poll_s) for cid in ids]


def airbyte_configured():
    return bool(os.getenv("AIRBYTE_CONNECTION_ID"))
