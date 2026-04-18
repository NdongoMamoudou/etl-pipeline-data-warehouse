-- int_order_items.sql
-- Regrouper les articles par commande

SELECT
    order_id,
    COUNT(order_item_id)    AS nb_articles,
    SUM(price)              AS total_amount,
    SUM(freight_value)      AS total_freight
FROM {{ ref('stg_order_items') }}
GROUP BY order_id