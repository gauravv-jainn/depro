# Demo script (about 8 minutes)

Run each block in a terminal from the repo root with the virtualenv active (`source .venv/bin/activate`).

## 0. Before the demo
```bash
pip install -r requirements.txt
cd orchestration && echo "AIRFLOW_UID=$(id -u)" > .env && docker compose up --build airflow-init && docker compose up -d && cd ..
```
For BigQuery also `export GCP_PROJECT=... GOOGLE_APPLICATION_CREDENTIALS=...` (see README, Step 2).

## 1. Architecture (1 min)
Source CSVs -> **Airbyte** (EL) -> warehouse `raw` -> **dbt** (staging -> intermediate -> marts) -> **Airflow** schedules and gates everything -> **conversational BI** on the marts. Show the diagram in `README.md`.

## 2. Orchestration: Airflow (2 min)
Open http://localhost:8080 (admin / admin), open `ecommerce_modern_data_pipeline`, show the Graph view, click **Trigger DAG**.
Point out: ingest -> `dbt_run` -> `dbt_test` -> refresh BI; retries; a failing `dbt_test` blocks the BI refresh.

## 3. Ingestion: Airbyte (1 min)
Open Airbyte (http://localhost:8000), show the connections, and say the first Airflow task triggers them through the Airbyte API (`ingestion/airbyte_trigger.py`).
If Airbyte is not set up: say plainly that the fallback loader is used in this run.

## 4. Transformation: dbt (2 min)
```bash
cd dbt_ecommerce && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir . --port 8089
```
Open http://localhost:8089 -> lineage graph (sources -> stg_* -> int_* -> marts). Show a `schema.yml` test, then in the Airflow log show `dbt test` PASS=26.

## 5. Conversational BI (2 min)
```bash
streamlit run bi_interface/app.py
```
Chat tab: click "Top selling products", "Delivery SLA by city", then type your own question, e.g. `top 5 customers by spend`. Open "View generated SQL". Dashboard tab: KPIs. Sidebar shows the active warehouse and row counts per layer.

## Questions you may get
- *Why ELT, not ETL?* Raw data lands untouched in the warehouse; transformations are versioned SQL in dbt.
- *What if a test fails?* `dbt_test` fails, downstream tasks are skipped, the BI refresh never runs.
- *How is the chat answered?* Rule-based intent parser to SQL on the marts; with `ANTHROPIC_API_KEY` set, other questions go to an LLM for text-to-SQL.
