# Optimisation SQL — Benchmark

## Requête analysée
Analyse des commandes par client (jointure fact_orders + dim_customers)

## Résultats

| Métrique | Avant | Après | Gain |
|----------|-------|-------|------|
| Temps d'exécution | 1537 ms | 713 ms | **-53%** |

## Optimisations appliquées

### 1. Index sur les clés de jointure
```sql
CREATE INDEX idx_fact_orders_customer_id 
ON dwh.fact_orders(customer_id);

CREATE INDEX idx_dim_customers_customer_id 
ON dwh.dim_customers(customer_id);
```

### Pourquoi ça accélère ?

Sans index → PostgreSQL scanne toute la table ligne par ligne
Avec index → PostgreSQL saute directement aux bonnes lignes

## Conclusion

Gain de 53% sur la requête principale grâce aux index
sur les clés de jointure.