# Airbyte Ingestion Guide: Source to Snowflake RAW

This guide documents the Data Ingestion (EL - Extract & Load) layer of our Modern Data Pipeline using **Airbyte** to ingest raw transactional data into our **Snowflake** data warehouse.

---

## 1. Architecture Overview (EL Layer)

```
[ E-Commerce Data Source ]
(CSV / Postgres / REST API)
           │
           ▼
     [ AIRBYTE ]  <--- Orchestrated via Airflow / Airbyte Trigger
  (Extract & Load)
           │
           ▼
[ SNOWFLAKE: ECOMMERCE_DB.RAW ]
  ├── RAW.CUSTOMERS
  ├── RAW.PRODUCTS
  ├── RAW.ORDERS
  ├── RAW.ORDER_ITEMS
  └── RAW.PAYMENTS
```

### Why Airbyte?
- **Separation of Concerns**: Decouples extraction/loading (EL) from transformation (T in dbt).
- **Connector Ecosystem**: 300+ pre-built connectors (Postgres, Stripe, Shopify, Google Ads, S3).
- **Schema Evolution**: Automatically handles new columns and nested JSON normalization.
- **ELT Philosophy**: Data lands in Snowflake in its rawest, most truthful form before business transformation.

---

## 2. Setting Up Airbyte (Cloud or OSS)

### Option A: Airbyte Cloud (Zero Installation, Recommended)
1. Sign up for a free account at [cloud.airbyte.com](https://cloud.airbyte.com) (includes 14-day free trial with unlimited credits).
2. **Create Destination**:
   - Select **Snowflake**.
   - **Host**: `<account_identifier>.snowflakecomputing.com`
   - **Role**: `DATA_ENGINEER_ROLE` or `ACCOUNTADMIN`
   - **Warehouse**: `ECOMMERCE_WH`
   - **Database**: `ECOMMERCE_DB`
   - **Default Schema**: `RAW`
   - **Username & Password**: Your Snowflake credentials.
   - **Loading Method**: Internal Stage or S3 Stage.
3. **Create Source**:
   - Select **File (CSV)** or **PostgreSQL** or **HTTP/API**.
   - Point to the raw CSV URL or upload files from `data_source/raw_data/`.
4. **Create Connection**:
   - Set Sync Frequency to **Manual** (we trigger it via Airflow) or **Every 24 Hours**.
   - Sync Mode: **Full Refresh | Overwrite** (or Incremental Append).
   - Click **Set up connection** and test the sync.

### Option B: Airbyte Open Source (Docker)
If running on a machine with Docker Desktop:
```bash
git clone https://github.com/airbytehq/airbyte.git
cd airbyte
./run-ab-platform.sh
```
Open `http://localhost:8000` (default login: `airbyte` / `password`).

---

## 3. Automated Ingestion Script (`snowflake_ingest.py`)

To ensure your college demonstration runs smoothly even without configuring live Airbyte webhooks, we have implemented `ingestion/snowflake_ingest.py`. 

This script:
1. Implements the exact same logic as an Airbyte sync connector.
2. Reads the raw datasets (`customers.csv`, `products.csv`, `orders.csv`, `order_items.csv`, `payments.csv`).
3. Appends audit metadata (`_ingested_at`, `_sync_id`).
4. Loads data directly into Snowflake `ECOMMERCE_DB.RAW` tables (or local high-performance DuckDB cache for offline viva demos).
