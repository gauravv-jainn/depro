-- Intermediate model: int_order_items_detailed
-- Joins order items with product catalog to compute cost, revenue, and gross profit margins

WITH items AS (
    SELECT * FROM staging.stg_order_items
),

products AS (
    SELECT * FROM staging.stg_products
),

joined AS (
    SELECT
        i.order_item_id,
        i.order_id,
        i.product_id,
        p.product_name,
        p.category,
        i.quantity_ordered,
        i.unit_price_inr,
        p.unit_cost_inr,
        i.discount_inr,
        i.line_total_inr AS gross_revenue_inr,
        ROUND((p.unit_cost_inr * i.quantity_ordered), 2) AS total_cost_inr,
        ROUND(i.line_total_inr - (p.unit_cost_inr * i.quantity_ordered), 2) AS gross_profit_inr,
        CASE
            WHEN i.line_total_inr > 0 
            THEN ROUND(((i.line_total_inr - (p.unit_cost_inr * i.quantity_ordered)) / i.line_total_inr) * 100, 2)
            ELSE 0.0
        END AS margin_percentage
    FROM items i
    LEFT JOIN products p ON i.product_id = p.product_id
)

SELECT * FROM joined;
