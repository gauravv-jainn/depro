-- Intermediate model: int_customer_orders_summary
-- Aggregates orders at customer level to compute RFM (Recency, Frequency, Monetary) metrics

WITH orders AS (
    SELECT * FROM staging.stg_orders
),

items AS (
    SELECT * FROM staging.stg_order_items
),

order_totals AS (
    SELECT
        order_id,
        SUM(line_total_inr) AS items_total_inr
    FROM items
    GROUP BY order_id
),

joined_orders AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_date,
        o.order_status,
        (COALESCE(ot.items_total_inr, 0.0) + o.shipping_fee_inr) AS order_total_inr
    FROM orders o
    LEFT JOIN order_totals ot ON o.order_id = ot.order_id
    WHERE o.order_status != 'Cancelled'
),

customer_agg AS (
    SELECT
        customer_id,
        COUNT(order_id) AS total_orders,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS most_recent_order_date,
        ROUND(SUM(order_total_inr), 2) AS lifetime_spend_inr,
        ROUND(AVG(order_total_inr), 2) AS average_order_value_inr
    FROM joined_orders
    GROUP BY customer_id
)

SELECT * FROM customer_agg;
