# Livrable 1 — Découverte des sources

## Contenu

- **Rapport principal** : `00_livrable_decouverte_sources.md`
- **Échantillons JSON (sanitisés)** : `samples/`
  - `airlabs_airports_sample.json`
  - `airlabs_schedules_sample.json`
  - `afklm_flightstatus_sample.json`
- **Scripts SQL (PostgreSQL)** : `sql/`
  - `01_create_schemas.sql`
  - `02_create_tables.sql`

## Lien avec le code du repo

- Ingestion AirLabs : `1_ingestion/ingestion_airlabs.py`
- Transformations Silver : `2_silver_processing/silver_flights_clean.py`
- Enrichissement géo : `2_silver_processing/silver_airports_geo.py`

