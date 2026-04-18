-- Nettoyage de la table orders brute
-- table   commande

SELECT
    order_id,
    customer_id,
    order_status AS status,
    order_purchase_timestamp::timestamp AS purchase_date,
    order_approved_at::timestamp AS approved_date,
    order_delivered_customer_date::timestamp AS delivered_date,
    order_estimated_delivery_date::timestamp AS estimated_delivery_date
FROM {{ source('staging', 'orders') }}
WHERE order_id IS NOT NULL
AND customer_id IS NOT NULL  