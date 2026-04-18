-- Nettoyage de la table sellers brute
-- table vendeur
SELECT
    seller_id,
    seller_city     AS city,
    seller_state    AS state
FROM {{ source('staging', 'sellers') }}
WHERE seller_id IS NOT NULL