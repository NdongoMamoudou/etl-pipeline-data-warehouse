-- Nettoyage de la table customers brute
SELECT
    customer_id,
    customer_unique_id,
    customer_city       AS city,
    customer_state      AS state
FROM {{ source('staging', 'customers') }}
WHERE customer_id IS NOT NULL