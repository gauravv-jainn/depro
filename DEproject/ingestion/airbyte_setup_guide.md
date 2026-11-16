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

## 2. Setting Up Airbyte (open source, local)

Verified with `abctl` v0.30.4 on Windows 11 / Docker Desktop (23 GB RAM). Airbyte runs a Kubernetes cluster inside Docker, so give Docker Desktop at least 12 GB. Do **not** use `--low-resource-mode` (not tested). Airbyte Cloud and Fivetran were not tested.

**Linux / macOS** (official installer):
```bash
curl -LsfS https://get.airbyte.com | bash -
DO_NOT_TRACK=1 abctl local install --no-browser --insecure-cookies   # several minutes the first time
abctl local credentials          # prints login email/password and client id/secret
```
**Windows:** the installer script targets Linux/macOS, but the same release ships a Windows build. Download `abctl-<version>-windows-amd64.zip` from https://github.com/airbytehq/abctl/releases, check it against `abctl_<version>_checksums.txt`, unzip, and run `abctl.exe local install --no-browser --insecure-cookies` (PowerShell: `& "C:\pathbctl.exe" local install ...`).

The UI is at http://localhost:8000. Log in with the email/password from `abctl local credentials`. Keep the client id/secret only in a git-ignored env file (`orchestration/.env`), never in the repo.

---

## 3. Wiring the five CSV tables into BigQuery

The Airbyte **File** source reads one file per source, so create **5 sources** (one per CSV) and **5 connections** to one BigQuery destination.

1. **Destination -> BigQuery** (`bigquery_raw`)
   - Project ID: your `GCP_PROJECT` (here `de-microproject`)
   - Dataset location: the same as `BQ_LOCATION` (default `US`)
   - Default dataset ID: `raw`
   - Loading method: Standard inserts
   - Service account key JSON: paste the contents of the key file (the service account needs *BigQuery Data Editor* and *BigQuery Job User*)
2. **Source -> File (CSV)** (repeat for each table)
   - Dataset name: `customers` (then `products`, `orders`, `order_items`, `payments`). This becomes the table name.
   - Format: `csv`, Storage provider: `HTTPS: Public Web`
   - URL: `https://raw.githubusercontent.com/gauravv-jainn/depro/main/DEproject/data_source/raw_data/customers.csv` (change the file name for each table; use your own public repo if you forked)
3. **Connection** (for each source): Destination = the BigQuery one, Schedule = **Manual** (Airflow triggers it), Sync mode = **Full refresh | Overwrite**.
4. Airbyte creates `raw.customers`, `raw.orders`, ... itself.

How this was done in the verified run: the destination was created in the Airbyte UI (the key was pasted there), while the 5 sources and 5 connections were created through the Airbyte public API (`/api/public/v1/sources` and `/connections`), which is equivalent to the UI steps above. The UI forms for sources and connections were not clicked through.

**The CSV URL must be publicly reachable.** If the GitHub repo is private, `raw.githubusercontent.com` answers 404 and Airbyte fails at schema discovery ("Failed to discover schemas ... 404 Client Error"). Make the repo public (or host the CSVs somewhere public) before creating the connections.

### What Airbyte writes to BigQuery (checked)
`raw.<table>` has the CSV columns plus `_airbyte_raw_id`, `_airbyte_extracted_at`, `_airbyte_meta`, `_airbyte_generation_id`. Type inference: ids/names/dates/timestamps stay `STRING`, numbers become `NUMERIC` (`quantity`, `unit_price`, `line_total`, `discount_amount`, `shipping_fee`, `amount`, `price`, `cost`, `inventory_stock`). Airbyte also creates an `airbyte_internal` dataset for its raw JSON staging tables. The dbt staging models cast every column explicitly (`CAST(... AS TIMESTAMP)`, `FLOAT64`, `INTEGER`), so they work unchanged on both these tables and on the fallback loader's tables; `dbt run` 12/12 and `dbt test` 26/26 passed on both, with identical mart numbers.

### BigQuery sandbox finding
The free sandbox (no billing account) blocks DML and streaming inserts. With **Standard inserts** the Airbyte BigQuery destination (full refresh | overwrite) still wrote all five tables successfully in this setup (150 / 30 / 600 / 1020 / 492 rows), so billing was **not** needed. If a future Airbyte version needs DML for its type-and-dedupe step and fails, the options are to enable billing on the project (the free tier still covers this data size) or to use the fallback loader below.

---

## 4. Letting Airflow trigger the syncs

Copy each connection id from its page URL (`/connections/<id>/...`) and set (in `orchestration/.env` for Docker):
```bash
AIRBYTE_URL=http://host.docker.internal:8000   # use http://localhost:8000 when running outside Docker
AIRBYTE_CONNECTION_ID=<id1>,<id2>,<id3>,<id4>,<id5>
AIRBYTE_CLIENT_ID=...                           # from `abctl local credentials`
AIRBYTE_CLIENT_SECRET=...
```
When `AIRBYTE_CONNECTION_ID` is set, the first Airflow task (`ingest_to_raw`) starts a sync for each connection one after another through the Airbyte public API (bearer token from `/api/public/v1/applications/token`, then `POST /api/public/v1/jobs`) and waits for each to succeed (`ingestion/airbyte_trigger.py`). dbt then runs on the freshly loaded `raw` tables. In the verified run the Airflow container reached Airbyte through `host.docker.internal:8000`, and the five jobs took about 50 seconds each.

---

## 5. Fallback loader and skipping ingestion

- If `AIRBYTE_CONNECTION_ID` is not set, the fallback loader (`ingestion/bigquery_ingest.py` or `local_ingest.py`) loads the CSVs itself, so the pipeline still runs without Airbyte. It writes to BigQuery when `GCP_PROJECT` is set, otherwise to a local DuckDB file. It is a stand-in for demos, not Airbyte, and it **overwrites** `raw.*` (including tables Airbyte loaded earlier).
- If `SKIP_INGEST=1`, the ingest task does nothing (no Airbyte, no fallback loader), so dbt runs on whatever is already in `raw`. See "Running without Airbyte on a small machine" in the README.
