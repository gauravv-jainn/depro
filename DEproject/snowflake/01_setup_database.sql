-- ====================================================================
-- MODERN DATA PIPELINE: SNOWFLAKE INFRASTRUCTURE SETUP
-- Database, Schemas, Warehouse & Access Setup
-- ====================================================================

-- 1. Create Dedicated Virtual Warehouse
CREATE WAREHOUSE IF NOT EXISTS ECOMMERCE_WH
    WITH 
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Virtual warehouse for e-commerce modern data pipeline ETL and BI queries';

-- Use the warehouse
USE WAREHOUSE ECOMMERCE_WH;

-- 2. Create Database
CREATE DATABASE IF NOT EXISTS ECOMMERCE_DB
    COMMENT = 'Enterprise e-commerce database for raw ingestion, dbt models, and BI reporting';

USE DATABASE ECOMMERCE_DB;

-- 3. Create Schemas following Medallion / Multi-layer Architecture
-- RAW Schema: Direct landing zone for Airbyte / Ingestion connectors
CREATE SCHEMA IF NOT EXISTS ECOMMERCE_DB.RAW
    COMMENT = 'Raw ingested data layer (Bronze level, source faithful)';

-- STAGING Schema: dbt staging models with cleaning and type casting
CREATE SCHEMA IF NOT EXISTS ECOMMERCE_DB.STAGING
    COMMENT = 'Staging and cleaned layer managed by dbt';

-- MARTS Schema: Business intelligence and dimensional models (Star Schema)
CREATE SCHEMA IF NOT EXISTS ECOMMERCE_DB.MARTS
    COMMENT = 'Dimensional models, facts, and aggregated business marts (Gold level)';

-- 4. Create Pipeline Service Role (Optional for RBAC / Production demo)
CREATE ROLE IF NOT EXISTS DATA_ENGINEER_ROLE;
GRANT USAGE ON WAREHOUSE ECOMMERCE_WH TO ROLE DATA_ENGINEER_ROLE;
GRANT ALL ON DATABASE ECOMMERCE_DB TO ROLE DATA_ENGINEER_ROLE;
GRANT ALL ON SCHEMA ECOMMERCE_DB.RAW TO ROLE DATA_ENGINEER_ROLE;
GRANT ALL ON SCHEMA ECOMMERCE_DB.STAGING TO ROLE DATA_ENGINEER_ROLE;
GRANT ALL ON SCHEMA ECOMMERCE_DB.MARTS TO ROLE DATA_ENGINEER_ROLE;

SELECT 'Snowflake infrastructure setup successfully completed!' AS STATUS;
