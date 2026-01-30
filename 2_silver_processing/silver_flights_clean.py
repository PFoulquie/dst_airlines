import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv
import pytz

load_dotenv()
PARIS_TZ = pytz.timezone('Europe/Paris')

def get_engine():
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    dbname = os.getenv("DB_NAME")
    url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(url)

engine = get_engine()

def process_airports_to_silver():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Traitement Silver : Airports (Historique)...")
    try:
        df_stg = pd.read_sql("SELECT * FROM staging.stg_projet_airports", engine)
        try:
            existing_codes = pd.read_sql("SELECT iata_code FROM acquisition.projet_airports", engine)['iata_code'].tolist()
        except:
            existing_codes = []

        df_to_insert = df_stg[~df_stg['iata_code'].isin(existing_codes)].copy()

        if not df_to_insert.empty:
            df_to_insert['created_at'] = datetime.now(PARIS_TZ)
            cols = ['iata_code', 'icao_code', 'name', 'lat', 'lng', 'created_at']
            df_to_insert[cols].to_sql('projet_airports', engine, schema='acquisition', if_exists='append', index=False)
            print(f"   -> Succès : {len(df_to_insert)} nouveaux aéroports ajoutés.")
        else:
            print("   -> Aucun nouvel aéroport à ajouter.")
    except Exception as e:
        print(r"   /!\ Erreur Silver Airports : " + f"{e}")

def process_schedules_to_silver():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Traitement Silver : Schedules (Historique via Upsert)...")
    try:
        df_stg = pd.read_sql("SELECT * FROM staging.stg_projet_schedules", engine)
        
        with engine.begin() as conn:
            for _, row in df_stg.iterrows():
                # On insère ou on met à jour le statut si le vol/heure existe déjà
                stmt = text("""
                    INSERT INTO acquisition.projet_flight_schedules 
                    (flight_iata, dep_iata, arr_iata, status, dep_time, arr_time, updated_at)
                    VALUES (:f, :d, :a, :s, :dt, :at, NOW())
                    ON CONFLICT (flight_iata, dep_time) 
                    DO UPDATE SET 
                        status = EXCLUDED.status,
                        arr_time = EXCLUDED.arr_time,
                        updated_at = NOW();
                """)
                conn.execute(stmt, {
                    "f": row['flight_iata'], "d": row['dep_iata'], 
                    "a": row['arr_iata'], "s": row['status'], 
                    "dt": row['dep_time'], "at": row.get('arr_time')
                })
        print(f"   -> Succès : {len(df_stg)} vols synchronisés sans perte d'historique.")
    except Exception as e:
        print(r"   /!\ Erreur Upsert Schedules : " + f"{e}")

if __name__ == "__main__":
    print("-" * 50)
    process_airports_to_silver()
    process_schedules_to_silver()
    print("-" * 50)