-- Gold Mart: mart_delivery_sla
-- City-level logistics fulfillment, SLA breach rate, and delivery duration analytics

WITH fact AS (
    SELECT * FROM marts.fact_orders
),

city_metrics AS (
    SELECT
        shipping_city,
        COUNT(order_id) AS total_orders,
        SUM(CASE WHEN order_status = 'Delivered' THEN 1 ELSE 0 END) AS delivered_orders,
        SUM(CASE WHEN order_status = 'Delivered' AND is_delayed_flag = 1 THEN 1 ELSE 0 END) AS delayed_orders,
        ROUND(
            (SUM(CASE WHEN order_status = 'Delivered' AND is_delayed_flag = 0 THEN 1 ELSE 0 END) * 100.0) / 
            NULLIF(SUM(CASE WHEN order_status = 'Delivered' THEN 1 ELSE 0 END), 0), 
            2
        ) AS on_time_delivery_rate_pct,
        ROUND(AVG(CASE WHEN order_status = 'Delivered' THEN fulfillment_duration_days ELSE NULL END), 2) AS avg_delivery_days,
        ROUND(AVG(CASE WHEN order_status = 'Delivered' AND is_delayed_flag = 1 THEN delivery_delay_days ELSE NULL END), 2) AS avg_delay_when_late_days
    FROM fact
    GROUP BY shipping_city
)

SELECT * FROM city_metrics
ORDER BY total_orders DESC;
