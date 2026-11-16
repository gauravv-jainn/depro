-- Gold Dimension: dim_products
-- Product catalog dimension enriched with sales performance, volume, and gross margin %

WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

item_sales AS (
    SELECT
        product_id,
        SUM(quantity_ordered) AS total_units_sold,
        SUM(gross_revenue_inr) AS total_revenue_inr,
        SUM(gross_profit_inr) AS total_profit_inr
    FROM {{ ref('int_order_items_detailed') }}
    GROUP BY product_id
),

final AS (
    SELECT
        p.product_id,
        p.product_name,
        p.category,
        p.retail_price_inr,
        p.unit_cost_inr,
        p.unit_profit_inr,
        p.stock_quantity AS current_inventory_stock,
        COALESCE(s.total_units_sold, 0) AS total_quantity_sold,
        COALESCE(s.total_revenue_inr, 0.0) AS total_gross_revenue,
        COALESCE(s.total_profit_inr, 0.0) AS total_gross_profit,
        CASE
            WHEN COALESCE(s.total_revenue_inr, 0) > 0
            THEN ROUND((COALESCE(s.total_profit_inr, 0) / s.total_revenue_inr) * 100, 2)
            ELSE ROUND(((p.retail_price_inr - p.unit_cost_inr) / p.retail_price_inr) * 100, 2)
        END AS gross_margin_percentage
    FROM products p
    LEFT JOIN item_sales s ON p.product_id = s.product_id
)

SELECT * FROM final