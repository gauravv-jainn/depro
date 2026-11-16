-- Staging model: stg_order_items
-- Cleans individual order items, discounts, and item-level revenue

WITH source AS (
    SELECT * FROM {{ source('raw', 'order_items') }}
),

cleaned AS (
    SELECT
        TRIM(order_item_id) AS order_item_id,
        TRIM(order_id) AS order_id,
        TRIM(product_id) AS product_id,
        CAST(quantity AS INTEGER) AS quantity_ordered,
        CAST(unit_price AS DOUBLE) AS unit_price_inr,
        COALESCE(CAST(discount_amount AS DOUBLE), 0.0) AS discount_inr,
        CAST(line_total AS DOUBLE) AS line_total_inr
    FROM source
    WHERE order_item_id IS NOT NULL
)

SELECT * FROM cleaned