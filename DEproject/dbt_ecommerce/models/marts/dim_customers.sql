-- Gold Dimension: dim_customers
-- Customer dimensional table with RFM behavioral segmentation and Lifetime Value (LTV)

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

summary AS (
    SELECT * FROM {{ ref('int_customer_orders_summary') }}
),

final AS (
    SELECT
        c.customer_id,
        c.full_name,
        c.email,
        c.phone_number,
        c.city,
        c.state,
        c.signup_at,
        COALESCE(s.total_orders, 0) AS total_orders_placed,
        COALESCE(s.lifetime_spend_inr, 0.0) AS lifetime_spend,
        COALESCE(s.average_order_value_inr, 0.0) AS average_order_value,
        s.first_order_date,
        s.most_recent_order_date,
        CASE
            WHEN s.lifetime_spend_inr >= 10000 THEN 'Platinum VIP'
            WHEN s.lifetime_spend_inr >= 5000 THEN 'Gold Loyal'
            WHEN s.lifetime_spend_inr >= 2000 THEN 'Silver Regular'
            WHEN s.total_orders >= 1 THEN 'Bronze New'
            ELSE 'Prospect (No Orders)'
        END AS customer_segment
    FROM customers c
    LEFT JOIN summary s ON c.customer_id = s.customer_id
)

SELECT * FROM final