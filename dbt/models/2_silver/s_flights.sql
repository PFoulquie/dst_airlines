{{ config(materialized='table', schema='silver') }}

WITH raw_flights AS (
    SELECT 
        jsonb_array_elements(payload->'operationalFlights') as f,
        ingested_at
    FROM {{ source('afklm_bronze', 'b_afklm_flights') }}
)

SELECT
    (f->>'id')::varchar as flight_id,
    (f->>'flightNumber')::integer as flight_number,
    (f->>'flightScheduleDate')::date as flight_date,
    (f->'airline'->>'code')::varchar as airline_code,
    (f->'route'->>0)::varchar as origin_iata,
    (f->'route'->>1)::varchar as destination_iata,
    (f->>'flightStatusPublic')::varchar as flight_status,
    ingested_at as technical_at
FROM raw_flights