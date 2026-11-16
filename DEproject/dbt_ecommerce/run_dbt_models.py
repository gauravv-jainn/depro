"""
Thin wrapper that runs the real dbt CLI (dbt run / dbt test) for this project.

Target selection:
  * DBT_TARGET env var if set ("duckdb" or "snowflake")
  * otherwise "snowflake" when SNOWFLAKE_ACCOUNT is set, else local "duckdb"
"""

import os
import shutil
import subprocess
import sys

DBT_DIR = os.path.abspath(os.path.dirname(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(DBT_DIR, ".."))


def _dbt_executable():
    """Prefer the dbt next to the running interpreter (venv), then PATH."""
    candidate = os.path.join(os.path.dirname(sys.executable), "dbt")
    if os.path.exists(candidate):
        return candidate
    found = shutil.which("dbt")
    if not found:
        raise RuntimeError("dbt not found. Run: pip install -r requirements.txt")
    return found


def resolve_target():
    if os.getenv("DBT_TARGET"):
        return os.environ["DBT_TARGET"]
    return "snowflake" if os.getenv("SNOWFLAKE_ACCOUNT") else "duckdb"


def _run_dbt(*args):
    env = dict(os.environ)
    env.setdefault("DUCKDB_PATH", os.path.join(PROJECT_ROOT, "warehouse.duckdb"))
    cmd = [_dbt_executable(), *args, "--project-dir", DBT_DIR,
           "--profiles-dir", DBT_DIR, "--target", resolve_target()]
    print("[dbt] $", " ".join(cmd), flush=True)
    result = subprocess.run(cmd, env=env, cwd=DBT_DIR)
    if result.returncode != 0:
        raise RuntimeError(f"`dbt {args[0]}` failed with exit code {result.returncode}")


def run_transformations():
    """`dbt run`: build staging (views), intermediate (views) and marts (tables)."""
    _run_dbt("run")


def run_dbt_data_tests():
    """`dbt test`: run all schema.yml tests (unique, not_null, accepted_values...)."""
    _run_dbt("test")


if __name__ == "__main__":
    run_transformations()
    run_dbt_data_tests()
