
  create view "postgres"."silver_raw"."flight_data__source_operational_flight_delays__dbt_tmp"
    
    
  as (
    -- raw.flight_data__source_operational_flight_delays
-- Entité : un événement de retard sur un leg. Un même leg peut avoir plusieurs codes retard
--          (ex. retard météo + retard technique = 2 lignes pour le même leg).
-- Source : table `operational_flight_delays` chargée par dlt depuis l'API AF/KLM.
-- Grain  : 1 ligne par événement de retard (N lignes possibles par leg).
-- Rôle   : aucun cast, aucune transformation. La durée (ISO 8601) est parsée en couche int.

select
    id,
    flight_leg_id,
    delay_code,
    delay_duration
    --delay_reason
from "postgres"."public"."operational_flight_delays"
  );