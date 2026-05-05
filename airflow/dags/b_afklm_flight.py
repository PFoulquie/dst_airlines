import os, requests, json, time, psycopg2, logging
from datetime import datetime
from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.exceptions import AirflowFailException
from dotenv import load_dotenv

# Configuration
load_dotenv("/opt/airflow/.env")
logger = logging.getLogger("airflow.task")

# Définition de l'Asset pour déclencher le Silver
BRONZE_FLIGHTS_ASSET = Dataset("psql://supabase/bronze/b_afklm_flights")

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
    tags=["afklm", "bronze", "production"]
)
def afklm_bronze():

    @task(outlets=[BRONZE_FLIGHTS_ASSET])
    def extract_and_load_bronze(ds=None):
        # --- 1. CONFIGURATION INITIALE ---
        base_url = os.getenv("AF_API_URL")
        key_1 = os.getenv("AF_CLIENT_ID_1")
        key_2 = os.getenv("AF_CLIENT_ID_2")
        endpoint = f"{base_url.rstrip('/')}/flightstatus"
        
        stats = {"success": 0, "errors": 0, "details": []}
        start_time = datetime.now()
        
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        try:
            # --- 2. INITIALISATION STRUCTURES ---
            cur.execute("CREATE SCHEMA IF NOT EXISTS bronze; CREATE SCHEMA IF NOT EXISTS logs;")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bronze.b_afklm_flights (
                    id SERIAL PRIMARY KEY, 
                    payload JSONB, 
                    extraction_date DATE, 
                    page_number INT, 
                    ingested_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)
            conn.commit()

            # --- 3. GESTION DE LA REPRISE (CHECKPOINT) ---
            cur.execute("SELECT COALESCE(MAX(page_number), -1) FROM bronze.b_afklm_flights WHERE extraction_date = %s", (ds,))
            start_page = cur.fetchone()[0] + 1
            
            # --- 4. APPEL API INITIAL (Vérification Quota/Access) ---
            current_key = key_1
            headers = {"API-Key": current_key}
            params = {"startRange": f"{ds}T00:00:00Z", "endRange": f"{ds}T23:59:59Z", "pageNumber": 0}
            
            try:
                r = requests.get(endpoint, headers=headers, params=params, timeout=30)
                
                # Failover initial
                if r.status_code == 403 and key_2:
                    logger.warning("[SECURITY] Quota Clé 1 dépassé au démarrage. Basculement sur Clé 2.")
                    current_key = key_2
                    headers["API-Key"] = current_key
                    r = requests.get(endpoint, headers=headers, params=params, timeout=30)
                
                if r.status_code == 403:
                    raise Exception("Toutes les clés API sont saturées (Quota 403).")
                
                r.raise_for_status()
                total_pages = r.json().get("page", {}).get("totalPages", 1)
                logger.info(f"[BRONZE] Début du traitement : {total_pages} pages à récupérer (Start: {start_page}).")
                
            except Exception as e:
                cur.execute("""
                    INSERT INTO logs.job_runs (job_name, layer, status, records_processed, records_error, error_details, execution_date, started_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, ('ingestion_afklm', 'BRONZE', 'FAILED', 0, 1, json.dumps({"critical_error": str(e)}), ds, start_time))
                conn.commit()
                raise AirflowFailException(f"Arrêt critique Bronze : {e}")

            # --- 5. BOUCLE D'INGESTION ---
            for page in range(start_page, total_pages):
                try:
                    params["pageNumber"] = page
                    res = requests.get(endpoint, headers=headers, params=params, timeout=30)
                    
                    # Sécurité Failover en cours de boucle
                    if res.status_code == 403:
                        if current_key == key_1 and key_2:
                            logger.warning(f"[SECURITY] Quota Clé 1 atteint à la page {page}. Basculement sur Clé 2...")
                            current_key = key_2
                            headers["API-Key"] = current_key
                            res = requests.get(endpoint, headers=headers, params=params, timeout=30)
                        else:
                            logger.error(f"[FATAL] Plus aucune clé API disponible à la page {page}. Arrêt forcé.")
                            stats["details"].append({"page": page, "error": "All API keys exhausted"})
                            break # On sort de la boucle de pages

                    if res.status_code == 200:
                        cur.execute("""
                            INSERT INTO bronze.b_afklm_flights (payload, extraction_date, page_number) 
                            VALUES (%s, %s, %s)
                        """, (json.dumps(res.json()), ds, page))
                        conn.commit()
                        stats["success"] += 1
                        logger.info(f"[BRONZE] [PAGE {page + 1}/{total_pages}] Insertion réussie.")
                    else:
                        stats["errors"] += 1
                        stats["details"].append({"page": page, "status": res.status_code})
                        logger.error(f"[BRONZE] [PAGE {page + 1}/{total_pages}] Échec (Code {res.status_code})")
                
                except Exception as e:
                    stats["errors"] += 1
                    stats["details"].append({"page": page, "error": str(e)})
                
                time.sleep(0.5)

            # --- 6. LOG DE FIN DE JOB ---
            final_status = "SUCCESS" if stats["errors"] == 0 and stats["success"] > 0 else "PARTIAL"
            if stats["success"] == 0: final_status = "FAILED"
            
            logger.info(f"[BRONZE] Job terminé. Statut: {final_status} | {stats['success']} pages récupérées.")

            cur.execute("""
                INSERT INTO logs.job_runs (job_name, layer, status, records_processed, records_error, error_details, execution_date, started_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, ('ingestion_afklm', 'BRONZE', final_status, stats["success"], stats["errors"], json.dumps(stats["details"]), ds, start_time))
            conn.commit()

        finally:
            cur.close()
            conn.close()

    extract_and_load_bronze()

afklm_bronze()