import psycopg2
import logging
from datetime import datetime
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

logger = logging.getLogger(__name__)

# --- IDENTIFIANTS EN DUR POUR LE TEST ---
# On utilise exactement les valeurs de ton .env
DB_CONFIG = {
    "host": "aws-1-eu-west-1.pooler.supabase.com",
    "port": "5432",
    "dbname": "postgres",
    "user": "postgres.amtxaysrmhlznfwqemdu",
    "password": "FormationData2026",
    "sslmode": "require",
}

@dag(
    dag_id="test_connexion_supabase",
    start_date=datetime(2026, 4, 20),
    schedule=None,
    catchup=False,
)
def test_supabase_dag():

    @task
    def check_db_connection():
        logger.info(f"Tentative de connexion à Supabase sur : {DB_CONFIG['host']}:{DB_CONFIG['port']}")

        try:
            # Tentative de connexion directe via psycopg2
            conn = psycopg2.connect(**DB_CONFIG)
            cur = conn.cursor()
            cur.execute("SELECT 1;")
            result = cur.fetchone()

            logger.info(f"SUCCÈS ! La base de données a répondu : {result}")

            cur.close()
            conn.close()
            return "OK"

        except Exception as e:
            logger.exception("Erreur lors de la connexion à Supabase")
            raise AirflowFailException(str(e))

    check_db_connection()

test_supabase_dag()