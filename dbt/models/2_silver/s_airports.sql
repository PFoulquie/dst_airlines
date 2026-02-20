{{ config(materialized='table', schema='silver') }}

WITH flight_legs AS (
    -- On extrait chaque segment de vol (leg) du JSON
    SELECT 
        jsonb_array_elements(jsonb_array_elements(payload->'operationalFlights')->'flightLegs') as leg
    FROM {{ source('afklm_bronze', 'b_afklm_flights') }}
),

all_locations AS (
    -- Extraction des aéroports de départ
    SELECT 
        (leg->'departureInformation'->'airport'->>'code')::varchar as airport_code,
        (leg->'departureInformation'->'airport'->>'name')::varchar as airport_name,
        (leg->'departureInformation'->'airport'->'city'->>'name')::varchar as city_name,
        (leg->'departureInformation'->'airport'->'city'->'country'->>'name')::varchar as country_name,
        (leg->'departureInformation'->'airport'->'location'->>'latitude')::float as latitude,
        (leg->'departureInformation'->'airport'->'location'->>'longitude')::float as longitude
    FROM flight_legs
    
    UNION -- Le UNION fusionne et gère une partie du dédoublonnage
    
    -- Extraction des aéroports d'arrivée
    SELECT 
        (leg->'arrivalInformation'->'airport'->>'code')::varchar as airport_code,
        (leg->'arrivalInformation'->'airport'->>'name')::varchar as airport_name,
        (leg->'arrivalInformation'->'airport'->'city'->>'name')::varchar as city_name,
        (leg->'arrivalInformation'->'airport'->'city'->'country'->>'name')::varchar as country_name,
        (leg->'arrivalInformation'->'airport'->'location'->>'latitude')::float as latitude,
        (leg->'arrivalInformation'->'airport'->'location'->>'longitude')::float as longitude
    FROM flight_legs
)

-- Dédoublonnage final par code IATA pour avoir une table de référence propre
SELECT DISTINCT ON (airport_code) * FROM all_locations
WHERE airport_code IS NOT NULL