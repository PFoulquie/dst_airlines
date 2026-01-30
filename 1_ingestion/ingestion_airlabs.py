import requests
import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def get_engine():
    url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    return create_engine(url)

engine = get_engine()

def ingest_to_staging(endpoint, params=None):
    url = f"https://airlabs.co/api/v9/{endpoint}"
    query_params = params or {}
    query_params['api_key'] = os.getenv("AIRLABS_API_KEY")
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Ingestion Staging : {endpoint}...")
    
    try:
        response = requests.get(url, params=query_params)
        if response.status_code == 200:
            data = response.json().get('response', [])
            if not data:
                print(f"   -> Attention : Aucune donnée reçue pour {endpoint}.")
                return
            
            df = pd.DataFrame(data)
            df['ingested_at'] = datetime.now()
            
            # NOMMAGE DEMANDÉ : staging.stg_projet_...
            table_name = f"stg_projet_{endpoint}"
            df.to_sql(table_name, engine, schema='staging', if_exists='replace', index=False)
            print(f"   -> Succès : {len(df)} lignes insérées dans staging.{table_name}.")
        else:
            print(r"   /!\ Erreur API " + f"{response.status_code} sur {endpoint}.")
    except Exception as e:
        print(r"   /!\ Erreur lors de l'ingestion de " + f"{endpoint} : {e}")

if __name__ == "__main__":
    print("-" * 50)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] DÉMARRAGE DU PIPELINE INGESTION")
    print("-" * 50)
    
    ingest_to_staging("airports", {"country_code": "FR"})
    ingest_to_staging("schedules", {"dep_iata": "NCE"})
    
    print("-" * 50)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] FIN DE L'INGESTION")
    print("-" * 50)