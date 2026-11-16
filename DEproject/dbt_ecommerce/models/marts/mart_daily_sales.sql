-- Gold Mart: mart_daily_sales
-- Aggregates daily operational KPIs: revenue, profit, average order value, order volume

WITH fact AS (
    SELECT * FROM {{ ref('fact_orders') }}
),

daily AS (
    SELECT
        order_date,
        COUNT(order_id) AS total_orders,
        COUNT(DISTINCT customer_id) AS unique_customers_ordering,
        SUM(CASE WHEN order_status = 'Delivered' THEN 1 ELSE 0 END) AS delivered_orders,
        SUM(CASE WHEN order_status = 'Cancelled' THEN 1 ELSE 0 END) AS cancelled_orders,
        ROUND(SUM(order_total_amount), 2) AS gross_revenue,
        ROUND(SUM(CASE WHEN order_status != 'Cancelled' THEN order_total_amount ELSE 0 END), 2) AS net_revenue,
        ROUND(SUM(CASE WHEN order_status != 'Cancelled' THEN net_gross_profit_amount ELSE 0 END), 2) AS total_profit,
        ROUND(AVG(CASE WHEN order_status != 'Cancelled' THEN order_total_amount ELSE NULL END), 2) AS average_order_value
    FROM fact
    GROUP BY order_date
)

SELECT * FROM daily
ORDER BY order_date ASC