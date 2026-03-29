import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# ==========================================
# CHARGEMENT DU FICHIER .env
# ==========================================
load_dotenv()

# ==========================================
# CONNEXION À POSTGRESQL
# ==========================================
user     = os.getenv("POSTGRES_USER", "etl_user")
password = os.getenv("POSTGRES_PASSWORD", "etl_password")
host     = os.getenv("POSTGRES_HOST", "localhost")
port     = os.getenv("POSTGRES_PORT", "5433")
db       = os.getenv("POSTGRES_DB", "etl_db")

print(f"Connexion à {host}:{port}/{db} avec l'utilisateur {user}")
engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

# ==========================================
# LISTE DES FICHIERS À CHARGER
# 1 fichier CSV = 1 table dans staging
# ==========================================
fichiers = {
    "orders":            "olist_orders_dataset.csv",
    "customers":         "olist_customers_dataset.csv",
    "products":          "olist_products_dataset.csv",
    "order_items":       "olist_order_items_dataset.csv",
    "order_payments":    "olist_order_payments_dataset.csv",
    "order_reviews":     "olist_order_reviews_dataset.csv",
    "sellers":           "olist_sellers_dataset.csv",
    "geolocation":       "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}

# ==========================================
# CHARGEMENT DE CHAQUE CSV DANS STAGING
# ==========================================
for table, fichier in fichiers.items():
    chemin = f"data/raw/{fichier}"

    print(f"\nChargement de {fichier}...")

    # Lecture du CSV
    df = pd.read_csv(chemin)
    print(f"  {len(df)} lignes lues")

    # Chargement dans staging
    df.to_sql(
        name=table,
        con=engine,
        schema="staging",
        if_exists="replace",
        index=False
    )

    print(f"   staging.{table} chargée !")

print("\n  Tous les fichiers sont chargés dans staging !")