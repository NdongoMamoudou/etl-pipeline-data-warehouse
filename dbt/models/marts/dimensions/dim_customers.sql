-- Dimension clients
SELECT
    customer_id,
    customer_unique_id,
    city,
    state
FROM {{ ref('stg_customers') }}