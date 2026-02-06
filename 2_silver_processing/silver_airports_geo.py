import requests
import pandas as pd
from sqlalchemy import create_engine, text
import time
from datetime import datetime
import pytz
import os
from dotenv import load_dotenv

# --- CONFIGURATION ---
load_dotenv() # Charge le fichier .env situé à la racine
PARIS_TZ = pytz.timezone('Europe/Paris')

def get_engine():
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    dbname = os.getenv("DB_NAME")
    # Utilise l'IP 192.168.93.58 définie dans ton .env
    url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(url)

engine = get_engine()

def get_now_paris():
    return datetime.now(PARIS_TZ)

def get_geo_info(lat, lng):
    try:
        url = f"https://nominatim.openstreetmap.org/reverse?format=json&lat={lat}&lon={lng}&addressdetails=1"
        headers = {'User-Agent': 'DST_Airlines_Enrichment_Bot/1.0'} 
        response = requests.get(url, headers=headers, timeout=5)
        if response.status_code == 200:
            address = response.json().get('address', {})
            city = address.get('city') or address.get('town') or address.get('village') or address.get('suburb') or 'Inconnu'
            postcode = address.get('postcode', '00000')
            return city, postcode
        return "Inconnu", "00000"
    except Exception:
        return "Inconnu", "00000"

def enrich_airports():
    print("-" * 50)
    print(f"[{get_now_paris().strftime('%H:%M:%S')}] DÉMARRAGE DE L'ENRICHISSEMENT GÉOGRAPHIQUE")
    
    query = """
        SELECT DISTINCT ON (iata_code) iata_code, name, lat, lng 
        FROM acquisition.projet_airports 
        WHERE city IS NULL OR city = 'Inconnu'
    """
    try:
        df_todo = pd.read_sql(query, engine)
    except Exception as e:
        print(r"  /!\ Erreur lors de la lecture de la table : " + f"{e}")
        return

    if df_todo.empty:
        print("-> Tout est déjà enrichi.")
        return

    print(f"-> {len(df_todo)} aéroports à localiser...")

    for _, row in df_todo.iterrows():
        city, pc = get_geo_info(row['lat'], row['lng'])
        now = get_now_paris()
        
        try:
            with engine.begin() as conn:
                stmt = text("""
                    UPDATE acquisition.projet_airports 
                    SET city = :city, postcode = :pc, last_updated_at = :now 
                    WHERE iata_code = :iata
                """)
                conn.execute(stmt, {"city": city, "pc": pc, "now": now, "iata": row['iata_code']})
            print(f"   MAJ : {row['iata_code']} -> {city}")
        except Exception as e:
            print(r"   /!\ Erreur SQL sur " + f"{row['iata_code']} : {e}")
            
        time.sleep(1.2) # Quota API Nominatim

if __name__ == "__main__":
    enrich_airports()