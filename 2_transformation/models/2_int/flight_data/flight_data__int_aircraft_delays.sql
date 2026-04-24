-- int.flight_data__int_aircraft_delays
-- Feature ML : Proportion de retards sur les sept derniers jours pour chaque appareil 
-- Calcule la proportion de vols en retard sur les sept jours précédant une date pour chaque modalité d'appareil
-- Est-ce que ce type d'appareil a tendance à provoquer du retard ? 
-- 1 ligne par (aircraftCode, flightScheduleDate).
{{ config(schema='int', materialized='view') }}

with flights_within_7_days as (
    select
        l.aircraftCode,
        d.delayDuration,
        l.cancelled,
        f.flightScheduleDate
    from {{ ref('flight_data__source_operational_flight_legs') }} l
    join {{ ref('flight_data__source_operational_flights') }} f on l.flightId = f.id
    join {{ ref('flight_data__source_operational_flight_delays') }} d on l.id = d.flightLegId
    where l.cancelled = 'N'
), 
select 
    fsd.aircraftCode,
    fsd.flightScheduleDate,
        sum(CASE WHEN fsd2.delayDuration != '00' THEN 1 ELSE 0 END) * 100.0 /
        NULLIF(COUNT(fsd2.delayDuration),0) as aircraftDelayedShare
FROM flights_within_7_days fsd left join flights_within_7_days fsd2 
    ON fsd.aircraftCode = fsd2.aircraftCode
        AND fsd2.flightScheduleDate BETWEEN fsd.flightScheduleDate - INTERVAL '7 days' AND fsd.flightScheduleDate
        GROUP BY
    fsd.aircraftCode,
    fsd.flightScheduleDate