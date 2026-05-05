import os
import traceback
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import create_engine, text
from typing import Optional

# 1. Chargement des variables d'environnement
load_dotenv()

DB_USER = os.getenv("DB_USER", "postgres.amtxaysrmhlznfwqemdu")
DB_PASSWORD = os.getenv("DB_PASSWORD", "FormationData2026")
DB_HOST = os.getenv("DB_HOST", "aws-1-eu-west-1.pooler.supabase.com")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

app = FastAPI(title="API AFKLM Silver")
engine = create_engine(DATABASE_URL)

@app.get("/")
def read_root():
    return {"message": " L'API est en ligne !"}

@app.get("/flight-stats")
def get_flight_stats(
    date: Optional[str] = Query(None),
    airline: Optional[str] = Query(None)
):
    try:
        with engine.connect() as conn:
            where_clauses = []
            params = {}
            
            # --- FILTRES ---
            if date:
                where_clauses.append("CAST(flight_date AS DATE) = CAST(:d AS DATE)")
                params["d"] = date
                
            if airline and airline != "ALL":
                where_clauses.append("airline_code = :a")
                params["a"] = airline
                
            where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
            
            # Affichage dans les logs pour debug
            print(f"Exécution requête avec filtres : {where_sql} | Paramètres : {params}")
            
            # --- REQUÊTES (Format universel row[0], row[1]) ---
            total = conn.execute(text(f"SELECT COUNT(*) FROM silver.s_flights {where_sql}"), params).scalar()
            
            status_res = conn.execute(text(f"SELECT flight_status, COUNT(*) FROM silver.s_flights {where_sql} GROUP BY flight_status"), params)
            statuses = {row[0]: row[1] for row in status_res}
            
            routes_res = conn.execute(text(f"SELECT origin_iata || ' ➔ ' || destination_iata, COUNT(*) as count FROM silver.s_flights {where_sql} GROUP BY origin_iata, destination_iata ORDER BY count DESC LIMIT 5"), params)
            top_routes = [{"route": row[0], "count": row[1]} for row in routes_res]

            airlines_res = conn.execute(text(f"SELECT airline_code, COUNT(*) as count FROM silver.s_flights {where_sql} GROUP BY airline_code ORDER BY count DESC LIMIT 5"), params)
            top_airlines = [{"airline_code": row[0], "count": row[1]} for row in airlines_res]

        return {
            "total_flights": total or 0,
            "statuses": statuses,
            "top_routes": top_routes,
            "top_airlines": top_airlines
        }
    except Exception as e:
        # En cas d'erreur, on affiche TOUT dans le terminal Docker
        print("ERREUR CRITIQUE DANS L'API :")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
    


@app.get("/monitoring-stats")
def get_monitoring_stats():
    try:
        with engine.connect() as conn:
            # Requête pour calculer les métriques d'observabilité
            query = text("""
                SELECT 
                    COUNT(DISTINCT execution_date) as days,
                    SUM(records_processed) as total_rows,
                    ROUND(CAST(SUM(records_error) AS NUMERIC) / NULLIF(SUM(records_processed) + SUM(records_error), 0) * 100, 2) as error_rate,
                    (SELECT status FROM logs.job_runs ORDER BY started_at DESC LIMIT 1) as last_status
                FROM logs.job_runs
                WHERE layer = 'BRONZE'
            """)
            res = conn.execute(query).mappings().first()
            
            return {
                "days": res["days"] or 0,
                "total_rows": res["total_rows"] or 0,
                "error_rate": float(res["error_rate"]) if res["error_rate"] else 0.0,
                "last_status": res["last_status"] or "N/A"
            }
    except Exception as e:
        print(f"ERREUR MONITORING : {e}")
        return {"days": 0, "total_rows": 0, "error_rate": 0, "last_status": "ERROR"}