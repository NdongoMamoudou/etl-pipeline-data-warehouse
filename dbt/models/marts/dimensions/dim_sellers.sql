-- Dimension vendeurs
SELECT
    seller_id,
    city,
    state
FROM {{ ref('stg_sellers') }}