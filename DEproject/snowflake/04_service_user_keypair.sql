-- ====================================================================
-- Service user for the pipeline (dbt, Airflow, Airbyte, BI) using key-pair auth.
-- Newer Snowflake accounts can block plain password logins for scripts, so use this.
--
-- 1. Generate a key pair on your machine:
--      openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out snowflake_key.p8 -nocrypt
--      openssl rsa -in snowflake_key.p8 -pubout -out snowflake_key.pub
-- 2. Paste the public key body (without the BEGIN/END lines) below, then run this as ACCOUNTADMIN.
-- 3. Set SNOWFLAKE_USER=PIPELINE_USER, SNOWFLAKE_ROLE=DATA_ENGINEER_ROLE and
--    SNOWFLAKE_PRIVATE_KEY_PATH=/absolute/path/to/snowflake_key.p8 in your env.
-- ====================================================================

USE ROLE ACCOUNTADMIN;

CREATE USER IF NOT EXISTS PIPELINE_USER
    TYPE = SERVICE
    DEFAULT_ROLE = DATA_ENGINEER_ROLE
    DEFAULT_WAREHOUSE = ECOMMERCE_WH
    RSA_PUBLIC_KEY = '<PASTE_PUBLIC_KEY_BODY_HERE>';

GRANT ROLE DATA_ENGINEER_ROLE TO USER PIPELINE_USER;

-- RAW tables were created by ACCOUNTADMIN; give the role access to them
GRANT ALL ON ALL TABLES IN SCHEMA ECOMMERCE_DB.RAW TO ROLE DATA_ENGINEER_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA ECOMMERCE_DB.RAW TO ROLE DATA_ENGINEER_ROLE;
