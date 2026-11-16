# Airbyte Ingestion Guide: Source to the BigQuery RAW layer

This guide documents the Data Ingestion (EL - Extract & Load) layer of our Modern Data Pipeline: **Airbyte** loads raw transactional data into the `raw` dataset of our **BigQuery** warehouse. dbt then builds `staging` and `marts` from it.

---

## 1. Architecture Overview (EL Layer)

```
[ E-Commerce Data Source ]
(CSV / Postgres / REST API)
           |
           v
     [ AIRBYTE ]  <--- triggered by the Airflow task `ingest_to_raw`
  (Extract & Load)
           |
           v
[ BIGQUERY: <project>.raw ]
  |- raw.customers
  |- raw.products
  |- raw.orders
  |- raw.order_items
  '- raw.payments
```

### Why Airbyte?
- **Separation of concerns**: extraction/loading (EL) is decoupled from transformation (T in dbt).
- **Connector ecosystem**: 300+ pre-built connectors (Postgres, Stripe, Shopify, Google Ads, S3).
- **Schema evolution**: new source columns are handled automatically.
- **ELT**: data lands in the warehouse in its rawest form before business transformation.

---

## 2. Setting Up Airbyte

Airbyte Cloud also works (cloud.airbyte.com), but the open-source install below runs locally and can be triggered from Airflow on the same machine.

### Option B: Airbyte Open Source (Docker, recommended for the demo)
Needs Docker Desktop running (about 4 CPUs / 8 GB RAM free).
```bash
curl -LsfS https://get.airbyte.com | bash -     # installs abctl
abctl local install                              # takes several minutes the first time
abctl local credentials                          # prints the login + client id/secret
```
Open `http://localhost:8000` and log in with the credentials printed above.

---

## 3. Wiring the five CSV tables into BigQuery

The Airbyte **File** source reads one file per source, so create **5 sources** (one per CSV) and **5 connections** to one BigQuery destination.

1. **Destination -> BigQuery**
   - Project ID: your `GCP_PROJECT`
   - Dataset location: the same as `BQ_LOCATION` (default `US`)
   - Default dataset ID: `raw`
   - Loading method: Standard inserts
   - Service account key JSON: paste the contents of the key file (service account needs the *BigQuery Data Editor* and *BigQuery Job User* roles)
2. **Source -> File (CSV)** (repeat for each table)
   - Dataset name: `customers` (then `products`, `orders`, `order_items`, `payments`). This becomes the table name.
   - Format: `csv`, Storage provider: `HTTPS: Public Web`
   - URL: `https://raw.githubusercontent.com/Palash2006-0/DEproject/main/DEproject/data_source/raw_data/customers.csv` (change the file name for each table)
3. **Connection** (for each source): Destination = the BigQuery one, Schedule = **Manual** (Airflow triggers it), Sync mode = **Full refresh | Overwrite**.
4. Airbyte creates `raw.customers`, `raw.orders`, ... itself. The dbt staging models only select the columns they need and cast them, so Airbyte's extra `_airbyte_*` columns are harmless.

> **Sandbox caveat:** the free BigQuery sandbox (no billing account) blocks DML and streaming inserts. Airbyte's BigQuery destination may need a billing-enabled project to write. If a sync fails for that reason, either enable billing (the free tier is still free for this data size) or leave `AIRBYTE_CONNECTION_ID` empty so the pipeline uses the fallback loader, which only uses load jobs.

## 4. Letting Airflow trigger the syncs

Copy each connection id from its page URL (`/connections/<id>/...`) and set:
```bash
AIRBYTE_URL=http://localhost:8000             # http://host.docker.internal:8000 inside Airflow's Docker
AIRBYTE_CONNECTION_ID=<id1>,<id2>,<id3>,<id4>,<id5>
AIRBYTE_CLIENT_ID=...                          # from `abctl local credentials`
AIRBYTE_CLIENT_SECRET=...
```
When `AIRBYTE_CONNECTION_ID` is set, the first Airflow task (`ingest_to_raw`) starts every sync through the Airbyte API and waits for them to succeed (`ingestion/airbyte_trigger.py`). dbt then runs on the freshly loaded `RAW` tables.

---

## 5. Fallback loader

If `AIRBYTE_CONNECTION_ID` is not set, the fallback loader (`ingestion/bigquery_ingest.py` or `local_ingest.py`) loads the CSVs itself, so the pipeline still runs without Airbyte. It writes to BigQuery when `GCP_PROJECT` is set, otherwise to a local DuckDB file. It is a stand-in for demos, not Airbyte.
