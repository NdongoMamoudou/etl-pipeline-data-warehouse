-- Table de faits principale
SELECT
    o.order_id,
    o.customer_id,
    o.status,
    o.purchase_date,
    o.approved_date,
    o.delivered_date,
    o.estimated_delivery_date
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('dim_customers') }} c
    ON o.customer_id = c.customer_id