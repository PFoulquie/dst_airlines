import os
import requests
import json
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def ingest_flights():
    # Variables
    api_key = os.getenv('AF_CLIENT_ID')
    db_uri = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    
    url = f"{os.getenv('AF_API_URL')}/flightstatus"
    headers = {'API-Key': api_key, 'Accept': 'application/hal+json'}

    print("🚀 Ingestion des vols (Bronze)...")
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        conn = psycopg2.connect(db_uri)
        cur = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bronze.b_afklm_flights (
                id SERIAL PRIMARY KEY,
                payload JSONB,
                ingested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
        """)
        cur.execute("INSERT INTO bronze.b_afklm_flights (payload) VALUES (%s)", (json.dumps(response.json()),))
        conn.commit()
        print("✅ Données vols insérées.")
        cur.close()
        conn.close()
    else:
        print(f"❌ Erreur API : {response.status_code}")

if __name__ == "__main__":
    ingest_flights()