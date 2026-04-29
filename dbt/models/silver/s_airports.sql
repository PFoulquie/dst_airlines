{{ config(
    materialized='incremental',
    unique_key='airport_code'
) }}

WITH unnested_flights AS (
    SELECT jsonb_array_elements(payload->'operationalFlights') as flight
    FROM {{ source('bronze', 'b_afklm_flights') }}
),
unnested_legs AS (
    SELECT jsonb_array_elements(flight->'flightLegs') as leg
    FROM unnested_flights
),
airports_raw AS (
    -- Extraction des aéroports de DÉPART
    SELECT 
        leg->'departureInformation'->'airport'->>'code' as airport_code,
        leg->'departureInformation'->'airport'->>'name' as airport_name,
        leg->'departureInformation'->'airport'->'city'->>'name' as city_name,
        leg->'departureInformation'->'airport'->'city'->'country'->>'name' as country_name,
        (leg->'departureInformation'->'airport'->'location'->>'latitude')::float8 as latitude,
        (leg->'departureInformation'->'airport'->'location'->>'longitude')::float8 as longitude
    FROM unnested_legs
    
    UNION
    
    -- Extraction des aéroports d'ARRIVÉE
    SELECT 
        leg->'arrivalInformation'->'airport'->>'code' as airport_code,
        leg->'arrivalInformation'->'airport'->>'name' as airport_name,
        leg->'arrivalInformation'->'airport'->'city'->>'name' as city_name,
        leg->'arrivalInformation'->'airport'->'city'->'country'->>'name' as country_name,
        (leg->'arrivalInformation'->'airport'->'location'->>'latitude')::float8 as latitude,
        (leg->'arrivalInformation'->'airport'->'location'->>'longitude')::float8 as longitude
    FROM unnested_legs
)

-- On déduplique proprement en gardant les infos les plus complètes (MAX)
SELECT 
    airport_code,
    MAX(airport_name) as airport_name,
    MAX(city_name) as city_name,
    MAX(country_name) as country_name,
    MAX(latitude) as latitude,
    MAX(longitude) as longitude
FROM airports_raw
WHERE airport_code IS NOT NULL
GROUP BY airport_code