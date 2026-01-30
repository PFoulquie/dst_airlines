import requests
import pandas as pd
from sqlalchemy import create_engine, text
import time
from datetime import datetime
import pytz

# --- CONFIGURATION ---
DB_URL = "postgresql://exploitation:xx@localhost:5432/xxx"
engine = create_engine(DB_URL)
PARIS_TZ = pytz.timezone('Europe/Paris')

def get_now_paris():
    """Retourne l'horodatage actuel au fuseau horaire de Paris."""
    return datetime.now(PARIS_TZ)

def get_geo_info(lat, lng):
    """Interroge l'API Nominatim (OpenStreetMap) pour le Reverse Geocoding."""
    try:
        url = f"https://nominatim.openstreetmap.org/reverse?format=json&lat={lat}&lon={lng}&addressdetails=1"
        headers = {'User-Agent': 'DST_Airlines_Enrichment_Bot/1.0'} 
        
        response = requests.get(url, headers=headers, timeout=5)
        if response.status_code == 200:
            address = response.json().get('address', {})
            # Logique de repli pour la ville
            city = address.get('city') or address.get('town') or address.get('village') or address.get('suburb') or 'Inconnu'
            postcode = address.get('postcode', '00000')
            return city, postcode
        return "Inconnu", "00000"
    except Exception:
        return "Inconnu", "00000"

def enrich_airports():
    start_time = get_now_paris()
    print("-" * 50)
    print(f"[{start_time.strftime('%H:%M:%S')}] DÉMARRAGE DE L'ENRICHISSEMENT GÉOGRAPHIQUE")
    
    # On cible les lignes sans ville et on dédoublonne à la lecture pour l'API
    query = """
        SELECT DISTINCT ON (iata_code) iata_code, name, lat, lng 
        FROM acquisition.projet_airports 
        WHERE city IS NULL OR city = 'Inconnu'
    """
    try:
        df_to_process = pd.read_sql(query, engine)
    except Exception as e:
        print(f"  /!\ Erreur lors de la lecture de la table : {e}")
        return

    total = len(df_to_process)
    print(f"-> {total} infrastructures uniques à enrichir.")

    if total == 0:
        print("-> Tout est déjà à jour. Fin du script.")
        return

    count = 0
    for index, row in df_to_process.iterrows():
        count += 1
        city, postcode = get_geo_info(row['lat'], row['lng'])
        now = get_now_paris()
        
        try:
            with engine.begin() as conn:
                # L'UPDATE par iata_code corrige toutes les lignes identiques d'un coup
                stmt = text("""
                    UPDATE acquisition.projet_airports 
                    SET city = :city, 
                        postcode = :pc, 
                        last_updated_at = :now 
                    WHERE iata_code = :iata
                """)
                conn.execute(stmt, {
                    "city": city, 
                    "pc": postcode, 
                    "now": now,
                    "iata": row['iata_code']
                })
            
            print(f"   [{count}/{total}] {row['iata_code']} ({row['name']}) -> {city}")
        
        except Exception as e:
            print(f"  /!\ Erreur SQL sur {row['iata_code']} : {e}")
        
        # Respect du quota Nominatim (1.2s pour être large)
        time.sleep(1.2)

    end_time = get_now_paris()
    print("-" * 50)
    print(f"[{end_time.strftime('%H:%M:%S')}] FIN DE L'ENRICHISSEMENT")

if __name__ == "__main__":
    enrich_airports()