-- Nettoyage de la table products brute
-- table produit
SELECT
    product_id,
    product_category_name           AS category,
    product_weight_g                AS weight_g,
    product_length_cm               AS length_cm,
    product_height_cm               AS height_cm,
    product_width_cm                AS width_cm
FROM {{ source('staging', 'products') }}
WHERE product_id IS NOT NULL