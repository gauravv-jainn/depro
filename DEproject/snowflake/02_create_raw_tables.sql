-- ====================================================================
-- RAW LANDING ZONE TABLES (ECOMMERCE_DB.RAW)
-- Target destination for Airbyte / Ingestion connectors
-- ====================================================================

USE DATABASE ECOMMERCE_DB;
USE SCHEMA RAW;

-- 1. Customers Raw Table
CREATE TABLE IF NOT EXISTS RAW.CUSTOMERS (
    customer_id VARCHAR(50),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(100),
    signup_date TIMESTAMP_NTZ,
    _ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 2. Products Raw Table
CREATE TABLE IF NOT EXISTS RAW.PRODUCTS (
    product_id VARCHAR(50),
    product_name VARCHAR(255),
    category VARCHAR(100),
    price FLOAT,
    cost FLOAT,
    inventory_stock INT,
    _ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 3. Orders Raw Table
CREATE TABLE IF NOT EXISTS RAW.ORDERS (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_date TIMESTAMP_NTZ,
    order_status VARCHAR(50),
    payment_method VARCHAR(50),
    shipping_city VARCHAR(100),
    shipping_fee FLOAT,
    estimated_delivery_date DATE,
    actual_delivery_date TIMESTAMP_NTZ,
    _ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 4. Order Items Raw Table
CREATE TABLE IF NOT EXISTS RAW.ORDER_ITEMS (
    order_item_id VARCHAR(50),
    order_id VARCHAR(50),
    product_id VARCHAR(50),
    quantity INT,
    unit_price FLOAT,
    discount_amount FLOAT,
    line_total FLOAT,
    _ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 5. Payments Raw Table
CREATE TABLE IF NOT EXISTS RAW.PAYMENTS (
    payment_id VARCHAR(50),
    order_id VARCHAR(50),
    payment_date TIMESTAMP_NTZ,
    payment_method VARCHAR(50),
    amount FLOAT,
    payment_status VARCHAR(50),
    _ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

SELECT 'Raw tables created successfully!' AS STATUS;
