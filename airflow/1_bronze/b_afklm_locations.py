import os
import requests
import json
import psycopg2
from dotenv import load_dotenv

# 1. Charger les variables d'environnement
load_dotenv()

def ingest_locations():
    # Configuration depuis le .env
    api_key = os.getenv('AF_CLIENT_ID')
    db_uri = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    
    # Correction de l'URL : on utilise l'endpoint standard 'locations'
    # Si AF_API_URL finit par /opendata, l'URL sera complète
    url = "https://api.airfranceklm.com/refdata/location/locations"
    headers = {
        'API-Key': api_key,
        'Accept': 'application/json'
    }

    print(f"🚀 Ingestion des aéroports (Bronze) via : {url}")
    
    try:
        response = requests.get(url, headers=headers)
        print(f"📡 Status Code API : {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            
            # Connexion à Supabase
            conn = psycopg2.connect(db_uri)
            cur = conn.cursor()
            
            # Création du schéma et de la table
            cur.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bronze.b_afklm_locations (
                    id SERIAL PRIMARY KEY,
                    payload JSONB,
                    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                );
            """)
            
            # Nettoyage et insertion (Full Load pour un référentiel)
            cur.execute("TRUNCATE TABLE bronze.b_afklm_locations;")
            cur.execute(
                "INSERT INTO bronze.b_afklm_locations (payload) VALUES (%s)",
                (json.dumps(data),)
            )
            
            conn.commit()
            print("✅ Succès : Référentiel aéroports inséré dans bronze.b_afklm_locations")
            
            cur.close()
            conn.close()
        else:
            print(f"❌ Erreur API ({response.status_code}) : {response.text}")

    except Exception as e:
        print(f"💥 Erreur critique lors de l'exécution : {e}")

if __name__ == "__main__":
    ingest_locations()