-- DST Airlines - Livrable 1
-- DDL PostgreSQL aligné avec les scripts Python du repo
--
-- Tables attendues par :
-- - 1_ingestion/ingestion_airlabs.py
-- - 2_silver_processing/silver_flights_clean.py
-- - 2_silver_processing/silver_airports_geo.py

-- =========================
-- STAGING (Bronze-like)
-- =========================

CREATE TABLE IF NOT EXISTS staging.stg_projet_airports (
  -- Champs minimums utilisés en Silver
  iata_code      text,
  icao_code      text,
  name           text,
  lat            double precision,
  lng            double precision,

  -- Champs fréquents AirLabs (optionnels)
  country_code   text,
  city           text,
  timezone       text,

  -- Meta ingestion
  ingested_at    timestamptz
);

CREATE TABLE IF NOT EXISTS staging.stg_projet_schedules (
  -- Champs minimums utilisés en Silver
  flight_iata    text,
  dep_iata       text,
  arr_iata       text,
  status         text,
  dep_time       timestamptz,
  arr_time       timestamptz,

  -- Champs fréquents AirLabs (optionnels)
  airline_iata   text,
  flight_number  text,
  dep_time_utc   timestamptz,
  arr_time_utc   timestamptz,
  dep_estimated  timestamptz,
  arr_estimated  timestamptz,
  dep_actual     timestamptz,
  arr_actual     timestamptz,

  -- Meta ingestion
  ingested_at    timestamptz
);

-- =========================
-- ACQUISITION (Silver)
-- =========================

CREATE TABLE IF NOT EXISTS acquisition.projet_airports (
  iata_code        text PRIMARY KEY,
  icao_code        text,
  name             text,
  lat              double precision,
  lng              double precision,

  -- Enrichissement géographique (Nominatim)
  city             text,
  postcode         text,

  created_at       timestamptz NOT NULL DEFAULT now(),
  last_updated_at  timestamptz
);

-- Table "facts" des horaires (Upsert)
CREATE TABLE IF NOT EXISTS acquisition.projet_flight_schedules (
  flight_iata   text NOT NULL,
  dep_iata      text,
  arr_iata      text,
  status        text,
  dep_time      timestamptz NOT NULL,
  arr_time      timestamptz,
  updated_at    timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT uq_projet_flight_schedules UNIQUE (flight_iata, dep_time)
);

-- FK optionnelles (à activer si référentiel aéroports complet)
-- ALTER TABLE acquisition.projet_flight_schedules
--   ADD CONSTRAINT fk_projet_flight_schedules_dep
--   FOREIGN KEY (dep_iata) REFERENCES acquisition.projet_airports(iata_code);
--
-- ALTER TABLE acquisition.projet_flight_schedules
--   ADD CONSTRAINT fk_projet_flight_schedules_arr
--   FOREIGN KEY (arr_iata) REFERENCES acquisition.projet_airports(iata_code);

