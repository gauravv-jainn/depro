-- Staging model: stg_payments
-- Cleans transaction records and payment status

WITH source AS (
    SELECT * FROM {{ source('raw', 'payments') }}
),

cleaned AS (
    SELECT
        TRIM(payment_id) AS payment_id,
        TRIM(order_id) AS order_id,
        CAST(payment_date AS TIMESTAMP) AS paid_at,
        TRIM(payment_method) AS payment_method,
        CAST(amount AS {{ type_double() }}) AS payment_amount_inr,
        TRIM(payment_status) AS payment_status
    FROM source
    WHERE payment_id IS NOT NULL
)

SELECT * FROM cleaned