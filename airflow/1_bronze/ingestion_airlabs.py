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

def ingest_to_staging(endpoint, params=None, mode='replace'):
    """
    Ingère les données dans le staging. 
    mode='replace' pour vider la table, mode='append' pour ajouter des données.
    """
    url = f"https://airlabs.co/api/v9/{endpoint}"
    query_params = params or {}
    query_params['api_key'] = os.getenv("AIRLABS_API_KEY")
    
    # On force la limite au max autorisé par le plan (100 pour le gratuit)
    if 'limit' not in query_params:
        query_params['limit'] = 100

    try:
        response = requests.get(url, params=query_params)
        if response.status_code == 200:
            data = response.json().get('response', [])
            if not data:
                print(f"   -> Info : Aucun vol trouvé pour {params.get('dep_iata', endpoint)}")
                return
            
            df = pd.DataFrame(data)
            df['ingested_at'] = datetime.now()
            
            # Nommage conforme : staging.stg_projet_...
            table_name = f"stg_projet_{endpoint}"
            
            # if_exists=mode permet de cumuler si on passe 'append'
            df.to_sql(table_name, engine, schema='staging', if_exists=mode, index=False)
            print(f"   -> Succès : {len(df)} lignes ({mode}) ajoutées pour {params.get('dep_iata', endpoint)}")
        else:
            print(f"   /!\\ Erreur API {response.status_code} sur {endpoint}")
    except Exception as e:
        print(f"   /!\\ Erreur lors de l'ingestion de {endpoint} : {e}")

if __name__ == "__main__":
    print("-" * 50)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] DÉMARRAGE DU PIPELINE INGESTION")
    print("-" * 50)
    
    # 1. Mise à jour du référentiel des aéroports (on remplace)
    ingest_to_staging("airports", {"country_code": "FR"}, mode='replace')
    
    # 2. Liste des plus gros aéroports de France
    grands_hubs = ["CDG", "ORY", "NCE", "LYS", "MRS", "TLS", "BOD", "NTE", "BSL", "MPL"]
    
    print(f"Collecte des vols pour {len(grands_hubs)} aéroports...")
    
    for i, code in enumerate(grands_hubs):
        # Pour le TOUT PREMIER aéroport de la boucle, on vide la table (replace)
        # Pour tous les suivants, on ajoute les lignes à la suite (append)
        current_mode = 'replace' if i == 0 else 'append'
        
        ingest_to_staging("schedules", {"dep_iata": code}, mode=current_mode)
    
    print("-" * 50)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] FIN DE L'INGESTION - STAGING PRÊT")
    print("-" * 50)