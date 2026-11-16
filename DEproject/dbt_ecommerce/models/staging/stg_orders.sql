-- Staging model: stg_orders
-- Standardizes orders, extracts delivery milestones, handles dates

WITH source AS (
    SELECT * FROM {{ source('raw', 'orders') }}
),

cleaned AS (
    SELECT
        TRIM(order_id) AS order_id,
        TRIM(customer_id) AS customer_id,
        CAST(order_date AS TIMESTAMP) AS ordered_at,
        CAST(CAST(order_date AS TIMESTAMP) AS DATE) AS order_date,
        TRIM(order_status) AS order_status,
        TRIM(payment_method) AS payment_method,
        TRIM(shipping_city) AS shipping_city,
        COALESCE(CAST(shipping_fee AS {{ type_double() }}), 0.0) AS shipping_fee_inr,
        CAST(CAST(estimated_delivery_date AS TIMESTAMP) AS DATE) AS estimated_delivery_date,
        CAST(NULLIF(CAST(actual_delivery_date AS {{ dbt.type_string() }}), '') AS TIMESTAMP) AS delivered_at
    FROM source
    WHERE order_id IS NOT NULL
)

SELECT * FROM cleaned