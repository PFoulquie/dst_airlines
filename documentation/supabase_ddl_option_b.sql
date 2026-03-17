-- DDL Option B : 3 tables normalisées pour le pipeline AF/KLM
-- À exécuter dans Supabase SQL Editor
-- Inclut aircraft_type_code pour le ML (plan pipeline_afklm_dlt_dbt_ml)

-- Schémas pour dbt (raw, int, mart)
CREATE SCHEMA IF NOT EXISTS raw;   -- dbt y crée les vues source (raw.flight_data__source_*)
CREATE SCHEMA IF NOT EXISTS int;   -- dbt y crée les modèles intermédiaires (int.flight_data__int_*)
CREATE SCHEMA IF NOT EXISTS mart;  -- dbt y crée faits et dimensions (mart.fct_flight_legs, dim_*)

-- Table 1/3 : dlt charge les vols (1 ligne = 1 vol API)
CREATE TABLE IF NOT EXISTS public.operational_flights (
  id VARCHAR(50) PRIMARY KEY,
  flight_number INTEGER,
  flight_schedule_date DATE,
  airline_code VARCHAR(3),
  airline_name TEXT,
  haul VARCHAR(20),
  route TEXT[],
  flight_status_public VARCHAR(50),
  fetched_at TIMESTAMPTZ DEFAULT NOW()
);

-- Table 2/3 : dlt charge les segments (1 ligne = 1 leg) — aircraft_type_code pour le ML
-- Pas de FK : dlt charge en parallèle, l'ordre n'est pas garanti
CREATE TABLE IF NOT EXISTS public.operational_flight_legs (
  id UUID PRIMARY KEY,
  flight_id VARCHAR(50),
  leg_order INTEGER,
  departure_airport_code VARCHAR(3),
  arrival_airport_code VARCHAR(3),
  published_status VARCHAR(50),
  scheduled_departure TIMESTAMPTZ,
  actual_departure TIMESTAMPTZ,
  scheduled_arrival TIMESTAMPTZ,
  actual_arrival TIMESTAMPTZ,
  scheduled_flight_duration VARCHAR(20),
  cancelled BOOLEAN DEFAULT FALSE,
  aircraft_type_code VARCHAR(10)
);

-- Table 3/3 : dlt charge les retards (1 ligne = 1 retard par leg)
-- Pas de FK : dlt charge en parallèle, l'ordre n'est pas garanti
CREATE TABLE IF NOT EXISTS public.operational_flight_delays (
  id UUID PRIMARY KEY,
  flight_leg_id UUID,
  delay_code VARCHAR(10),
  delay_duration VARCHAR(20)
);

-- Index pour accélérer les JOINs dbt (legs→flights, delays→legs)
CREATE INDEX IF NOT EXISTS idx_operational_flight_legs_flight_id ON public.operational_flight_legs(flight_id);
CREATE INDEX IF NOT EXISTS idx_operational_flight_delays_flight_leg_id ON public.operational_flight_delays(flight_leg_id);
