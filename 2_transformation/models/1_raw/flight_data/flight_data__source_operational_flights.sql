-- raw.flight_data__source_operational_flights
-- Entité : un vol = une combinaison (numéro de vol, date, compagnie).
-- Un vol peut être composé de plusieurs legs (tronçons physiques).
-- Source : table `operational_flights` chargée par dlt depuis l'API AF/KLM.
-- Grain  : 1 ligne par vol.
-- Rôle   : typage uniquement (int, date, timestamptz). Aucune transformation métier.
{{ config(schema='raw', materialized='view') }}
select
    id,
    flightNumber::int as flightNumber,
    flightScheduleDate::date as flightScheduleDate,
    airlineCode,
    airlineName,
    haul,
    route,
    flightStatusPublic,
    fetchedAt::timestamptz as fetchedAt
from {{ source('flight_data', 'operational_flights') }}
