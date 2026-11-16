-- Staging model: stg_products
-- Standardizes product catalog, calculates initial unit margins

WITH source AS (
    SELECT * FROM {{ source('raw', 'products') }}
),

cleaned AS (
    SELECT
        TRIM(product_id) AS product_id,
        TRIM(product_name) AS product_name,
        TRIM(category) AS category,
        CAST(price AS {{ type_double() }}) AS retail_price_inr,
        CAST(cost AS {{ type_double() }}) AS unit_cost_inr,
        ROUND(CAST(price AS {{ type_double() }}) - CAST(cost AS {{ type_double() }}), 2) AS unit_profit_inr,
        CAST(inventory_stock AS INTEGER) AS stock_quantity
    FROM source
    WHERE product_id IS NOT NULL
)

SELECT * FROM cleaned