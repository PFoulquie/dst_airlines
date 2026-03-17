-- Migration : accepter les strings de dlt (casting fait en dbt 1_raw)
-- À exécuter dans Supabase SQL Editor si les tables ont déjà le DDL Option B (DATE, UUID)
-- dlt envoie des strings → on convertit les colonnes en VARCHAR
-- Les FK sont supprimées : dlt charge en parallèle, l'ordre n'est pas garanti

-- 1. Supprimer toutes les FK (chargement parallèle dlt incompatible avec les FK)
ALTER TABLE public.operational_flight_delays
  DROP CONSTRAINT IF EXISTS operational_flight_delays_flight_leg_id_fkey;

ALTER TABLE public.operational_flight_legs
  DROP CONSTRAINT IF EXISTS operational_flight_legs_flight_id_fkey;

-- 2. Passer les colonnes en VARCHAR pour accepter les strings de dlt
ALTER TABLE public.operational_flights
  ALTER COLUMN flight_schedule_date TYPE VARCHAR(50)
  USING flight_schedule_date::text;

ALTER TABLE public.operational_flight_legs
  ALTER COLUMN id TYPE VARCHAR(50)
  USING id::text;

ALTER TABLE public.operational_flight_delays
  ALTER COLUMN id TYPE VARCHAR(50) USING id::text,
  ALTER COLUMN flight_leg_id TYPE VARCHAR(50) USING flight_leg_id::text;
