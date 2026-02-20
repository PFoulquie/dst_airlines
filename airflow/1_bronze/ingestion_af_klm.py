import os
import json
import time
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# --- CONFIGURATION VIA VARIABLES D'ENVIRONNEMENT ---
AF_API_KEY = os.getenv('AF_API_KEY', 'yvsvffam3jgh6u3qurgu6dgk')
AF_BASE_URL = "https://api.airfranceklm.com/opendata/flightstatus"
DB_CONN_ID = "supabase_conn"

def fetch_and_load_to_supabase(ds, **kwargs):
    """
    ds: Date d'exécution automatique d'Airflow (format YYYY-MM-DD)
    """
    pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    headers = {
        'API-Key': AF_API_KEY,
        'Accept': 'application/hal+json'
    }
    
    # 1. Premier appel pour connaître le volume (pagination)
    # On filtre sur la journée d'exécution pour être incrémental
    params = {
        'startRange': f"{ds}T00:00:00Z",
        'endRange': f"{ds}T23:59:59Z",
        'pageNumber': 0
    }
    
    response = requests.get(AF_BASE_URL, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    
    total_pages = data.get('page', {}).get('totalPages', 1)
    print(f"Début de l'ingestion pour le {ds} : {total_pages} pages trouvées.")

    # 2. Boucle de pagination
    for page in range(total_pages):
        params['pageNumber'] = page
        res = requests.get(AF_BASE_URL, headers=headers, params=params)
        
        if res.status_code == 200:
            page_data = res.json()
            
            # Insertion du JSON brut dans Supabase
            insert_sql = """
                INSERT INTO bronze.afklm_flights (payload, extraction_date, page_number)
                VALUES (%s, %s, %s)
            """
            pg_hook.run(insert_sql, parameters=(json.dumps(page_data), ds, page))
            print(f"Page {page} insérée avec succès.")
        
        # Respect du quota : 1 appel par seconde
        time.sleep(1.2)

# --- DÉFINITION DU DAG ---
default_args = {
    'owner': 'formation_data',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ingestion_afklm_bronze_v1',
    default_args=default_args,
    description='Collecte JSON API AF-KLM vers Supabase Bronze',
    schedule_interval='@daily', # Se lance chaque nuit pour la veille
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['afklm', 'bronze', 'supabase']
) as dag:

    task_ingest = PythonOperator(
        task_id='fetch_api_and_push_to_supabase',
        python_callable=fetch_and_load_to_supabase,
    )