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
# 🔥 Lancement direct depuis Airflow
# ==========================================
task_dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt --log-path /tmp/dbt_logs",
    dag=dag,
)

# ==========================================
# TÂCHE 3 — DBT TEST
# 🔥 Lancement direct depuis Airflow
# ==========================================
task_dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir /opt/airflow/dbt --log-path /tmp/dbt_logs",
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