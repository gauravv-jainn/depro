# Demo script (about 8 minutes)

Run each block in a terminal from the project folder (the one containing `README.md`) with the virtualenv active (`source .venv/bin/activate`; Windows PowerShell: `.\.venv\Scripts\Activate.ps1`).

What was really run end to end (2026-11-17, Windows 11): Airbyte -> BigQuery `raw` -> dbt (12 models, 26 tests) -> Airflow DAG green -> Streamlit BI on BigQuery. Not tested: macOS / 8 GB laptop, the LLM text-to-SQL path. See "Verification status" in `README.md`.

## 0. Before the demo
```bash
pip install -r requirements.txt
cd orchestration
# create .env: AIRFLOW_UID=$(id -u) (Linux/macOS) or AIRFLOW_UID=50000 (Windows), plus the BigQuery / Airbyte
# variables from README Step 3, then:
docker compose up --build airflow-init && docker compose up -d && cd ..
```
For BigQuery also `export GCP_PROJECT=... GOOGLE_APPLICATION_CREDENTIALS=...` (see README, Step 2).
Airbyte (`abctl local install ...`) must already be running with its 5 connections if you want the ingest task to trigger it; the ingest task takes about 4-5 minutes (5 syncs, run one after another). On a small machine without Airbyte: set `SKIP_INGEST=1` (README, "Running without Airbyte on a small machine") and the BigQuery `raw` data stays as Airbyte loaded it.

## 1. Architecture (1 min)
Source CSVs -> **Airbyte** (EL) -> warehouse `raw` -> **dbt** (staging -> intermediate -> marts) -> **Airflow** schedules and gates everything -> **conversational BI** on the marts. Show the diagram in `README.md`.

## 2. Orchestration: Airflow (2 min)
Open http://localhost:8080 (admin / admin), open `ecommerce_modern_data_pipeline`, show the Graph view, click **Trigger DAG**.
Point out: ingest -> `dbt_run` -> `dbt_test` -> refresh BI; retries; a failing `dbt_test` blocks the BI refresh (shown once with a temporary failing test: `dbt_test` failed and the BI refresh went to `upstream_failed`). A full run with Airbyte takes about 5 minutes, so trigger it first and talk through the other parts while it runs.

## 3. Ingestion: Airbyte (1 min)
Open Airbyte (http://localhost:8000), show the 5 connections (manual schedule, Full refresh | Overwrite) and a successful sync, and say the first Airflow task triggers them through the Airbyte API (`ingestion/airbyte_trigger.py`).
If Airbyte is not running: say plainly that the fallback loader is used in this run (or that `SKIP_INGEST=1` reuses the data Airbyte loaded earlier).

## 4. Transformation: dbt (2 min)
```bash
cd dbt_ecommerce && dbt docs generate --profiles-dir . --target bigquery && dbt docs serve --profiles-dir . --target bigquery --port 8089
```
Open http://localhost:8089, open a model (e.g. `fact_orders`) and click the lineage-graph button at the bottom right (sources -> stg_* -> int_* -> marts). Show a `schema.yml` test, then in the Airflow log show `dbt test` PASS=26.

## 5. Conversational BI (2 min)
```bash
streamlit run bi_interface/app.py
```
Chat tab: click "Top selling products", "Delivery SLA by city", then type your own question, e.g. `top 5 customers by spend`. Open "View generated SQL". Dashboard tab: KPIs. Sidebar shows the active warehouse and row counts per layer.

## Questions you may get
- *Why ELT, not ETL?* Raw data lands untouched in the warehouse; transformations are versioned SQL in dbt.
- *What if a test fails?* `dbt_test` fails, downstream tasks are skipped, the BI refresh never runs.
- *How is the chat answered?* Rule-based intent parser to SQL on the marts; with `ANTHROPIC_API_KEY` set, other questions go to an LLM for text-to-SQL.
