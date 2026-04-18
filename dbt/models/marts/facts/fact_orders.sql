SELECT
    -- Clés
    o.order_id,
    o.customer_id,

    -- Infos commande
    o.status,
    o.purchase_date,
    o.approved_date,
    o.delivered_date,
    o.estimated_delivery_date,

    -- Mesures (depuis int_order_items)
    oi.nb_articles,
    oi.total_amount,
    oi.total_freight,

    -- Infos client (depuis stg_customers)
    c.city          AS customer_city,
    c.state         AS customer_state,

    -- Calcul délai de livraison (en jours)
    DATEDIFF('day', o.purchase_date, o.delivered_date) AS delivery_days

FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('int_order_items') }} oi 
    ON o.order_id = oi.order_id
LEFT JOIN {{ ref('stg_customers') }} c 
    ON o.customer_id = c.customer_id