import os
import requests
import json
import time
import psycopg2
import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowFailException
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv("/opt/airflow/.env")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "sslmode": "require",
}

@dag(
    dag_id="b_afklm_ingestion_full",
    start_date=datetime(2026, 4, 20),
    schedule='0 8 * * *',
    catchup=True,
    max_active_runs=1,
    tags=["afklm", "production"]
)
def afklm_pipeline():

    @task
    def extract_and_load_bronze(ds=None):
        base_url = os.getenv("AF_API_URL")
        key_1 = os.getenv("AF_CLIENT_ID_1")
        key_2 = os.getenv("AF_CLIENT_ID_2")
        
        if not base_url or not key_1:
            raise AirflowFailException("Variables API manquantes")

        endpoint = f"{base_url.rstrip('/')}/flightstatus"
        current_key = key_1

        # Connexion & Init
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bronze.b_afklm_flights (
                id SERIAL PRIMARY KEY, payload JSONB, extraction_date DATE,
                page_number INT, ingested_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        conn.commit()

        # Reprise
        cur.execute("SELECT COALESCE(MAX(page_number), -1) FROM bronze.b_afklm_flights WHERE extraction_date = %s", (ds,))
        start_page = cur.fetchone()[0] + 1
        
        headers = {"API-Key": current_key}
        params = {"startRange": f"{ds}T00:00:00Z", "endRange": f"{ds}T23:59:59Z", "pageNumber": 0}

        # Calcul du total
        try:
            r = requests.get(endpoint, headers=headers, params=params, timeout=30)
            if r.status_code == 403 and key_2:
                current_key = key_2
                headers["API-Key"] = current_key
                r = requests.get(endpoint, headers=headers, params=params, timeout=30)
            r.raise_for_status()
            total_pages = r.json().get("page", {}).get("totalPages", 1)
        except Exception as e:
            logger.error(f"Erreur init API : {e}")
            raise AirflowFailException("API injoignable")

        # Boucle Robuste
        for page in range(start_page, total_pages):
            try:
                params["pageNumber"] = page
                res = requests.get(endpoint, headers=headers, params=params, timeout=30)

                if res.status_code == 403 and current_key == key_1 and key_2:
                    current_key = key_2
                    headers["API-Key"] = current_key
                    res = requests.get(endpoint, headers=headers, params=params, timeout=30)

                if res.status_code == 200:
                    cur.execute("INSERT INTO bronze.b_afklm_flights (payload, extraction_date, page_number) VALUES (%s, %s, %s)", 
                                (json.dumps(res.json()), ds, page))
                    conn.commit()
                    logger.info(f"Page {page} insérée.")
                else:
                    logger.error(f"Saut page {page} (Code {res.status_code})")
            except:
                continue
            time.sleep(1)

        cur.close()
        conn.close()

    # Transformation Silver via le venv dbt
    silver_layer = BashOperator(
        task_id="dbt_silver_run",
        bash_command="""
            export DBT_LOG_PATH=/tmp/dbt_logs && 
            export DBT_TARGET_PATH=/tmp/dbt_target && 
            /home/airflow/dbt_venv/bin/dbt run \
            --project-dir /opt/airflow/dbt \
            --profiles-dir /opt/airflow/dbt \
            --select path:models/silver
        """,
        env={**os.environ}
    )

    extract_and_load_bronze() >> silver_layer

afklm_pipeline()