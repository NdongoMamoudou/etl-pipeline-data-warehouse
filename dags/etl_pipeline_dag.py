# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator
# from datetime import datetime, timedelta
# import subprocess
# import sys
# import os

# # ==========================================
# # CONFIGURATION DU DAG
# # ==========================================
# default_args = {
#     'owner': 'etl_user',
#     'retries': 1,                           # 1 retry si échec
#     'retry_delay': timedelta(minutes=5),    # attendre 5 min avant retry
#     'email_on_failure': False,
# }

# dag = DAG(
#     dag_id='etl_pipeline',                  # nom du DAG
#     description='Pipeline ETL complet : Extract → dbt run → dbt test',
#     default_args=default_args,
#     start_date=datetime(2024, 1, 1),
#     schedule_interval='0 8 * * *',          # tous les jours à 08h00
#     catchup=False,                          # ne pas rattraper les runs manqués
#     tags=['etl', 'dbt', 'portfolio'],
# )

# # ==========================================
# # TÂCHE 1 — EXTRACTION
# # Charge les CSV dans staging
# # ==========================================
# def run_extract():
#     print("Lancement de extract.py...")
#     result = subprocess.run(
#         ['python', '/opt/airflow/data/extract.py'],
#         capture_output=True,
#         text=True
#     )
#     print(result.stdout)
#     if result.returncode != 0:
#         raise Exception(f"Erreur extract.py : {result.stderr}")
#     print("Extraction terminée ✅")

# task_extract = PythonOperator(
#     task_id='extract_load',
#     python_callable=run_extract,
#     dag=dag,
# )

# # ==========================================
# # TÂCHE 2 — DBT RUN
# # Transforme les données staging → dwh
# # ==========================================
# task_dbt_run = BashOperator(
#     task_id='dbt_run',
#     bash_command='docker exec etl_dbt dbt run --project-dir /dbt --profiles-dir /dbt',
#     dag=dag,
# )

# # ==========================================
# # TÂCHE 3 — DBT TEST
# # Vérifie la qualité des données
# # ==========================================
# task_dbt_test = BashOperator(
#     task_id='dbt_test',
#     bash_command='docker exec etl_dbt dbt test --project-dir /dbt --profiles-dir /dbt',
#     dag=dag,
# )

# # ==========================================
# # TÂCHE 4 — NOTIFICATION
# # Confirme que le pipeline est terminé
# # ==========================================
# def notify_success():
#     print("Pipeline ETL terminé avec succès ✅")
#     print(f"Date : {datetime.now()}")

# task_notify = PythonOperator(
#     task_id='notify_success',
#     python_callable=notify_success,
#     dag=dag,
# )

# # ==========================================
# # ORDRE D'EXÉCUTION DES TÂCHES
# # ==========================================
# task_extract >> task_dbt_run >> task_dbt_test >> task_notify



# ==========================================
# IMPORTS
# ==========================================
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess

# ==========================================
# CONFIGURATION DU DAG
# ==========================================
default_args = {
    "owner": "etl_user",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

dag = DAG(
    dag_id="etl_pipeline",
    description="Pipeline ETL complet : Extract -> dbt run -> dbt test",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 8 * * *",
    catchup=False,
    tags=["etl", "dbt", "portfolio"],
)

# ==========================================
# TÂCHE 1 — EXTRACTION
# ==========================================
def run_extract():
    print("Lancement de extract.py...")

    result = subprocess.run(
        ["python", "/opt/airflow/data/extract.py"],
        capture_output=True,
        text=True
    )

    print(result.stdout)

    if result.returncode != 0:
        raise Exception(f"Erreur extract.py : {result.stderr}")

    print("Extraction terminée ✅")


task_extract = PythonOperator(
    task_id="extract_load",
    python_callable=run_extract,
    dag=dag,
)

# ==========================================
# TÂCHE 2 — DBT RUN
# Lance dbt dans le conteneur etl_dbt
# ==========================================
task_dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command="docker exec etl_dbt dbt run --project-dir /dbt --profiles-dir /dbt",
    dag=dag,
)

# ==========================================
# TÂCHE 3 — DBT TEST
# ==========================================
task_dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command="docker exec etl_dbt dbt test --project-dir /dbt --profiles-dir /dbt",
    dag=dag,
)

# ==========================================
# TÂCHE 4 — NOTIFICATION
# ==========================================
def notify_success():
    print("Pipeline ETL terminé avec succès ✅")
    print(f"Date : {datetime.now()}")


task_notify = PythonOperator(
    task_id="notify_success",
    python_callable=notify_success,
    dag=dag,
)

# ==========================================
# PIPELINE
# ==========================================
task_extract >> task_dbt_run >> task_dbt_test >> task_notify