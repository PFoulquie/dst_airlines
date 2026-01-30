import requests
import json
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import pytz

# --- CONFIGURATION ---
API_KEY = "2a1e8135-dc6d-4e7a-89f7-6760eef2da56"
# Note : À terme, remplace localhost par le nom du service docker si tu lances le script via Docker
DB_URL = "postgresql://exploitation:xxxxx=@localhost:5432/xxxx"
engine = create_engine(DB_URL)
PARIS_TZ = pytz.timezone('Europe/Paris')

def get_now_paris():
    """Retourne l'horodatage actuel précis pour le fuseau horaire de Paris."""
    return datetime.now(PARIS_TZ)

def fetch_from_airlabs(endpoint, params=None):
    """Récupère les données et les archive en Staging (Bronze)."""
    url = f"https://airlabs.co/api/v9/{endpoint}"
    query_params = {"api_key": API_KEY}
    if params: query_params.update(params)
    
    print(f"[{get_now_paris().strftime('%H:%M:%S')}] Appel API : /{endpoint}...")
    try:
        response = requests.get(url, params=query_params)
        response.raise_for_status()
        data = response.json()
        
        # Archivage Staging (Bronze) - On peut aussi l'envoyer vers MongoDB ici plus tard
        return data.get('response', [])
    except Exception as e:
        print(rf"  /!\ ERREUR API sur /{endpoint} : {e}")
        return []

# --- 1. RÉFÉRENTIEL INFRA (Airports) ---
print("-" * 50)
print("MAJ RÉFÉRENTIEL INFRASTRUCTURES")
data_airports = fetch_from_airlabs("airports", {"country_code": "FR"})

if data_airports:
    df_new = pd.DataFrame(data_airports)
    now = get_now_paris()
    try:
        existing_codes = pd.read_sql("SELECT iata_code FROM acquisition.projet_airports", engine)['iata_code'].tolist()
    except:
        existing_codes = []

    # Uniquement les nouveaux pour ne pas écraser le created_at original
    df_to_insert = df_new[~df_new['iata_code'].isin(existing_codes)].copy()

    if not df_to_insert.empty:
        df_to_insert['created_at'] = now
        df_to_insert['city'] = None
        df_to_insert['postcode'] = None
        cols = ['iata_code', 'icao_code', 'name', 'country_code', 'lat', 'lng', 'created_at', 'city', 'postcode']
        df_to_insert[cols].to_sql('projet_airports', engine, schema='acquisition', if_exists='append', index=False)
        print(f"-> {len(df_to_insert)} nouveaux aéroports ajoutés.")
    else:
        print("-> Référentiel déjà à jour.")

# --- 2. VOLS (Schedules) - LOGIQUE UPSERT ---
print("-" * 50)
print("COLLECTE ET MISE À JOUR DES VOLS")
data_vols = fetch_from_airlabs("schedules", {"dep_iata": "NCE"})

if data_vols:
    df_api = pd.DataFrame(data_vols)
    now = get_now_paris()
    
    df_api_clean = pd.DataFrame({
        'flight_iata': df_api.get('flight_iata'),
        'dep_iata': df_api.get('dep_iata'),
        'arr_iata': df_api.get('arr_iata'),
        'scheduled_dep': pd.to_datetime(df_api.get('dep_time')),
        'status': df_api.get('status'),
        'created_at': now,
        'last_updated_at': now
    }).drop_duplicates(subset=['flight_iata', 'scheduled_dep'])

    try:
        existing_vols = pd.read_sql("SELECT flight_iata, scheduled_dep, status FROM acquisition.projet_flight_schedules", engine)
        existing_vols['scheduled_dep'] = pd.to_datetime(existing_vols['scheduled_dep'])

        df_merged = df_api_clean.merge(existing_vols, on=['flight_iata', 'scheduled_dep'], how='left', indicator=True)

        # Nouveaux vols
        df_to_insert = df_merged[df_merged['_merge'] == 'left_only'].drop(columns=['_merge', 'status_y']).rename(columns={'status_x': 'status'})
        if not df_to_insert.empty:
            df_to_insert.to_sql('projet_flight_schedules', engine, schema='acquisition', if_exists='append', index=False)
            print(f"-> {len(df_to_insert)} nouveaux vols insérés.")

        # Mises à jour (si le statut a changé)
        df_to_update = df_merged[(df_merged['_merge'] == 'both') & (df_merged['status_x'] != df_merged['status_y'])].copy()
        if not df_to_update.empty:
            with engine.begin() as conn:
                for _, row in df_to_update.iterrows():
                    stmt = text("""
                        UPDATE acquisition.projet_flight_schedules 
                        SET status = :status, last_updated_at = :now
                        WHERE flight_iata = :flight AND scheduled_dep = :dep
                    """)
                    conn.execute(stmt, {"status": row['status_x'], "now": now, "flight": row['flight_iata'], "dep": row['scheduled_dep']})
            print(f"-> {len(df_to_update)} statuts de vols mis à jour.")
    except Exception as e:
        df_api_clean.to_sql('projet_flight_schedules', engine, schema='acquisition', if_exists='append', index=False)

print("-" * 50)
print(f"[{get_now_paris().strftime('%H:%M:%S')}] FIN DU TRAITEMENT")