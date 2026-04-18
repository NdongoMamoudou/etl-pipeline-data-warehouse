-- table details_commande

SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date::timestamp AS shipping_limit_date,
    price,
    freight_value
FROM {{ source('staging', 'order_items') }}
WHERE order_id IS NOT NULL
  AND product_id IS NOT NULL
  AND seller_id IS NOT NULL