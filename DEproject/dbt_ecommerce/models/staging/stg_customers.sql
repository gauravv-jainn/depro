-- Staging model: stg_customers
-- Cleans and standardizes raw customer data

WITH source AS (
    SELECT * FROM raw.customers
),

cleaned AS (
    SELECT
        TRIM(customer_id) AS customer_id,
        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,
        TRIM(first_name) || ' ' || TRIM(last_name) AS full_name,
        LOWER(TRIM(email)) AS email,
        TRIM(phone) AS phone_number,
        TRIM(city) AS city,
        TRIM(state) AS state,
        CAST(signup_date AS TIMESTAMP) AS signup_at
    FROM source
    WHERE customer_id IS NOT NULL
)

SELECT * FROM cleaned;
