import os, psycopg2, logging
from datetime import datetime
from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator
from dotenv import load_dotenv

load_dotenv("/opt/airflow/.env")
BRONZE_FLIGHTS_ASSET = Dataset("psql://supabase/bronze/b_afklm_flights")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"), "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"), "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"), "sslmode": "require",
}

@dag(dag_id="s_afklm_transformation_silver", start_date=datetime(2026, 4, 20), schedule=[BRONZE_FLIGHTS_ASSET], catchup=False, tags=["afklm", "silver"])
def afklm_silver():

    silver_layer = BashOperator(
        task_id="dbt_silver_run",
        bash_command="""
            export DBT_LOG_PATH=/tmp/dbt_logs && 
            export DBT_TARGET_PATH=/tmp/dbt_target && 
            /home/airflow/dbt_venv/bin/dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --select path:models/silver
        """
    )

    @task
    def log_silver_finish(ds=None):
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        try:
            cur.execute("INSERT INTO logs.job_runs (job_name, layer, status, execution_date, started_at) VALUES (%s, %s, %s, %s, NOW())",
                        ('dbt_transform_silver', 'SILVER', 'SUCCESS', ds))
            conn.commit()
        finally:
            cur.close()
            conn.close()

    silver_layer >> log_silver_finish()

afklm_silver()