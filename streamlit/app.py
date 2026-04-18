import streamlit as st
import snowflake.connector
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

# ==========================================
# CONNEXION SNOWFLAKE
# ==========================================
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema="dwh"
    )

@st.cache_data
def run_query(query):
    conn = get_connection()
    return pd.read_sql(query, conn)

# ==========================================
# DASHBOARD
# ==========================================
st.title("📦 Dashboard E-Commerce")
st.markdown("Analyse des commandes — Dataset Olist Brazil")

# ==========================================
# KPIs
# ==========================================
st.subheader("KPIs principaux")

kpi = run_query("""
    SELECT
        COUNT(*)                        AS total_commandes,
        ROUND(SUM(total_amount), 2)     AS chiffre_affaires,
        ROUND(AVG(total_amount), 2)     AS panier_moyen,
        ROUND(AVG(delivery_days), 1)    AS delai_moyen_livraison
    FROM dwh.fact_orders
    WHERE status = 'delivered'
""")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total commandes", f"{kpi['TOTAL_COMMANDES'][0]:,}")
col2.metric("Chiffre d'affaires", f"R$ {kpi['CHIFFRE_AFFAIRES'][0]:,}")
col3.metric("Panier moyen", f"R$ {kpi['PANIER_MOYEN'][0]}")
col4.metric("Délai moyen", f"{kpi['DELAI_MOYEN_LIVRAISON'][0]} jours")

# ==========================================
# GRAPHIQUE 1 — Commandes par statut
# ==========================================
st.subheader("Commandes par statut")

statut = run_query("""
    SELECT status, COUNT(*) AS nb_commandes
    FROM dwh.fact_orders
    GROUP BY status
    ORDER BY nb_commandes DESC
""")

st.bar_chart(statut.set_index("STATUS")["NB_COMMANDES"])

# ==========================================
# GRAPHIQUE 2 — Revenus par mois
# ==========================================
st.subheader("Revenus par mois")

revenus = run_query("""
    SELECT
        DATE_TRUNC('month', purchase_date) AS mois,
        ROUND(SUM(total_amount), 2)        AS revenus
    FROM dwh.fact_orders
    WHERE status = 'delivered'
    GROUP BY mois
    ORDER BY mois
""")

st.line_chart(revenus.set_index("MOIS")["REVENUS"])

# ==========================================
# GRAPHIQUE 3 — Top 10 villes
# ==========================================
st.subheader("Top 10 villes par nombre de commandes")

villes = run_query("""
    SELECT customer_city, COUNT(*) AS nb_commandes
    FROM dwh.fact_orders
    WHERE status = 'delivered'
    GROUP BY customer_city
    ORDER BY nb_commandes DESC
    LIMIT 10
""")

st.bar_chart(villes.set_index("CUSTOMER_CITY")["NB_COMMANDES"])