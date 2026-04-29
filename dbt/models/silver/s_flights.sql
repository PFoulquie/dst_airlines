{{ config(
    materialized='incremental',
    unique_key='flight_id'
) }}

WITH source_data AS (
    SELECT 
        ingested_at,
        payload->'operationalFlights' as flights_array
    FROM {{ source('bronze', 'b_afklm_flights') }}
    {% if is_incremental() %}
       -- On ne prend que les pages ingérées après la dernière technical_at de Silver
       -- C'est l'optimisation cruciale pour éviter le timeout
       WHERE ingested_at > (SELECT MAX(technical_at) FROM {{ this }})
    {% endif %}
),

unnested_data AS (
    SELECT 
        ingested_at,
        jsonb_array_elements(flights_array) as flight
    FROM source_data
),

raw_data AS (
    SELECT 
        flight->>'id' as flight_id,
        (flight->>'flightNumber')::int as flight_number,
        (flight->>'flightScheduleDate')::date as flight_date,
        flight->'airline'->>'code' as airline_code,
        flight->'route'->>0 as origin_iata,
        flight->'route'->>(jsonb_array_length(flight->'route') - 1)::int as destination_iata,
        flight->>'flightStatusPublic' as flight_status,
        ingested_at as technical_at,
        md5(CAST((
            flight->>'flightStatusPublic', 
            flight->'route'->>0, 
            flight->'route'->>(jsonb_array_length(flight->'route') - 1)::int
        ) AS TEXT)) as record_hash
    FROM unnested_data
)

SELECT * FROM raw_data
WHERE flight_id IS NOT NULL