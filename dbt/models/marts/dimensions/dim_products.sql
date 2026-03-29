-- Dimension produits
SELECT
    product_id,
    category,
    weight_g,
    length_cm,
    height_cm,
    width_cm
FROM {{ ref('stg_products') }}