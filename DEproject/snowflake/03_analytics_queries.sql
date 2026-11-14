-- ====================================================================
-- E-COMMERCE MODERN DATA WAREHOUSE - ANALYTICAL SHOWCASE QUERIES
-- Used for validation, viva demonstration & Conversational BI verification
-- ====================================================================

USE DATABASE ECOMMERCE_DB;
USE SCHEMA MARTS;

-- 1. Executive Revenue & Order KPIs
SELECT 
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND(SUM(order_total_amount), 2) AS gross_merchandise_value_inr,
    ROUND(AVG(order_total_amount), 2) AS average_order_value_inr,
    ROUND(AVG(total_items_in_order), 1) AS avg_basket_size,
    ROUND(AVG(delivery_delay_days), 2) AS avg_delivery_delay_days
FROM MARTS.FACT_ORDERS;

-- 2. Category Performance & Profit Margins
SELECT 
    category,
    COUNT(DISTINCT product_id) AS active_products,
    SUM(total_quantity_sold) AS total_units_sold,
    ROUND(SUM(total_gross_revenue), 2) AS total_revenue_inr,
    ROUND(SUM(total_gross_profit), 2) AS total_profit_inr,
    ROUND(AVG(gross_margin_percentage), 2) AS avg_margin_pct
FROM MARTS.DIM_PRODUCTS
GROUP BY category
ORDER BY total_revenue_inr DESC;

-- 3. Customer Lifetime Value (LTV) & RFM Tier Breakdown
SELECT 
    customer_segment,
    COUNT(customer_id) AS total_customers,
    ROUND(SUM(lifetime_spend), 2) AS segment_revenue_inr,
    ROUND(AVG(lifetime_spend), 2) AS avg_customer_ltv_inr,
    ROUND(AVG(total_orders_placed), 1) AS avg_orders_per_customer
FROM MARTS.DIM_CUSTOMERS
GROUP BY customer_segment
ORDER BY segment_revenue_inr DESC;

-- 4. Delivery SLA Performance by Shipping City
SELECT 
    shipping_city,
    total_orders,
    delayed_orders,
    on_time_delivery_rate_pct,
    avg_delivery_days
FROM MARTS.MART_DELIVERY_SLA
ORDER BY total_orders DESC;

-- 5. Daily Sales Run-Rate & Payment Channel Popularity
SELECT 
    order_date,
    total_orders,
    gross_revenue,
    net_revenue,
    top_payment_method
FROM MARTS.MART_DAILY_SALES
ORDER BY order_date DESC
LIMIT 15;
