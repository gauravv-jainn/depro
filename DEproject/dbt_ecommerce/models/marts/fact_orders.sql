-- Gold Fact: fact_orders
-- Central transactional sales fact table with financial and delivery fulfillment metrics

WITH orders AS (
    SELECT * FROM staging.stg_orders
),

item_metrics AS (
    SELECT
        order_id,
        COUNT(order_item_id) AS distinct_products_in_order,
        SUM(quantity_ordered) AS total_items_in_order,
        SUM(gross_revenue_inr) AS subtotal_revenue_inr,
        SUM(discount_inr) AS total_discount_inr,
        SUM(total_cost_inr) AS total_cogs_inr,
        SUM(gross_profit_inr) AS total_gross_profit_inr
    FROM staging.int_order_items_detailed
    GROUP BY order_id
),

final AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.ordered_at,
        o.order_date,
        o.order_status,
        o.payment_method,
        o.shipping_city,
        o.shipping_fee_inr,
        o.estimated_delivery_date,
        o.delivered_at,
        COALESCE(im.distinct_products_in_order, 0) AS distinct_products_in_order,
        COALESCE(im.total_items_in_order, 0) AS total_items_in_order,
        COALESCE(im.subtotal_revenue_inr, 0.0) AS items_subtotal_amount,
        COALESCE(im.total_discount_inr, 0.0) AS order_discount_amount,
        (COALESCE(im.subtotal_revenue_inr, 0.0) + o.shipping_fee_inr) AS order_total_amount,
        COALESCE(im.total_cogs_inr, 0.0) AS total_cogs_amount,
        COALESCE(im.total_gross_profit_inr, 0.0) AS net_gross_profit_amount,
        CASE
            WHEN o.delivered_at IS NOT NULL AND o.delivered_at > CAST(o.estimated_delivery_date AS TIMESTAMP)
            THEN 1
            ELSE 0
        END AS is_delayed_flag,
        CASE
            WHEN o.delivered_at IS NOT NULL
            THEN DATEDIFF('day', CAST(o.ordered_at AS DATE), CAST(o.delivered_at AS DATE))
            ELSE NULL
        END AS fulfillment_duration_days,
        CASE
            WHEN o.delivered_at IS NOT NULL AND o.delivered_at > CAST(o.estimated_delivery_date AS TIMESTAMP)
            THEN DATEDIFF('day', o.estimated_delivery_date, CAST(o.delivered_at AS DATE))
            ELSE 0
        END AS delivery_delay_days
    FROM orders o
    LEFT JOIN item_metrics im ON o.order_id = im.order_id
)

SELECT * FROM final;
