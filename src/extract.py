import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# ==========================================
# CHARGEMENT DU FICHIER .env
# ==========================================
load_dotenv()  # lit le fichier .env automatiquement

# ==========================================
# CONNEXION À POSTGRESQL
# ==========================================
user     = os.getenv("POSTGRES_USER", "etl_user")
password = os.getenv("POSTGRES_PASSWORD", "etl_password")
host     = os.getenv("POSTGRES_HOST", "localhost")
port     = os.getenv("POSTGRES_PORT", "5432")
db       = os.getenv("POSTGRES_DB", "etl_db")

print(f"Connexion à {host}:{port}/{db} avec l'utilisateur {user}")

engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

# ==========================================
# LECTURE DU CSV
# ==========================================
print("Lecture du fichier clients.csv...")
df = pd.read_csv("data/raw/clients.csv")

print(f"Nombre de lignes lues : {len(df)}")
print(df)

# ==========================================
# CHARGEMENT DANS STAGING
# ==========================================
print("\nChargement dans staging.clients...")
df.to_sql(
    name="clients",
    con=engine,
    schema="staging",
    if_exists="replace",
    index=False
)

print("✅ Données chargées dans staging.clients !")