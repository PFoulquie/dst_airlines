-- raw.flight_data__source_operational_flight_legs
-- Entité : un leg = un tronçon physique d'un vol entre deux aéroports (décollage → atterrissage).
-- Un vol peut avoir plusieurs legs (ex. CDG → NBO → JNB = 2 legs pour le même vol).
-- Source : table `operational_flight_legs` chargée par dlt depuis l'API AF/KLM.
-- Grain  : 1 ligne par leg (leg_order distingue les tronçons d'un même vol).
-- Rôle   : cast des timestamps en timestamptz. Aucune transformation métier.
{{ config(schema='raw', materialized='view') }}
select
    id,
    flightId,
    legOrder,
    departureAirportCode,
    arrivalAirportCode,
    departureAirportName,
    arrivalAirportName,
    publishedStatus,
    scheduledDeparture::timestamptz as scheduledDeparture,
    actualDeparture::timestamptz as actualDeparture,
    scheduledArrival::timestamptz as scheduledArrival,
    actualArrival::timestamptz as actualArrival,
    scheduledFlightDuration,
    cancelled,
    aircraftCode,
    aircraftName,
    departureCityCode,
    departureCityName,
    departureCountryCode,
    departureCountryName,
    arrivalCityCode,
    arrivalCityName,
    arrivalCountryCode,
    arrivalCountryName
from {{ source('flight_data', 'operational_flight_legs') }}
