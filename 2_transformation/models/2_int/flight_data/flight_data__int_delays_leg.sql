-- int.flight_data__int_delays_leg
-- Assemble les trois sources raw en une entité cohérente par leg.
-- Transformations clés :
--   - JOIN flights → legs : enrichit chaque tronçon avec les infos vol (numéro, date, compagnie).
--   - Parse ISO 8601 : convertit delay_duration (ex. "PT25M") en entier minutes.
--   - Agrégation delays : plusieurs codes retard par leg → 1 ligne (somme des durées, premier code).
--   - Calcul des écarts : departure/arrival_delay_minutes depuis les timestamps réels vs prévus.
-- Grain : 1 ligne par leg.
{{ config(schema='int', materialized='view') }}

with legs as (
    select
        l.id as legId,
        l.flightId,
        f.flightNumber,
        f.flightScheduleDate,
        l.scheduledDeparture,
        l.scheduledArrival,
        l.actualDeparture,
        l.actualArrival,
        l.scheduledFlightDuration,
        l.aircraftTypeCode,
        f.airlineCode,
        l.departureAirportCode,
        l.arrivalAirportCode,
        l.legOrder,
        l.cancelled,
    from {{ ref('flight_data__source_operational_flight_legs') }} l
    join {{ ref('flight_data__source_operational_flights') }} f on l.flightId = f.id
),
delay_parsed as (
    select
        flightLegId,
        delayCode,
        {{ parse_iso8601_duration_minutes('delayDuration') }} as delayMin
    from {{ ref('flight_data__source_operational_flight_delays') }}
    where delay_duration is not null
),
delay_agg as (
    select
        flightLegId,
        sum(delayMin)::int as delayDurationMinutes,
        min(delayCode) as delayCode
    from delay_parsed
    group by flightLegId
)
select
    legs.legId,
    legs.flightId,
    legs.flightNumber,
    legs.flightScheduleDate,
    legs.airlineCode,
    legs.legOrder,
    legs.departureAirportCode,
    legs.arrivalAirportCode,
    legs.published_status,
    legs.scheduledDeparture,
    legs.actualDeparture,
    legs.scheduledArrival,
    legs.actualArrival,
    legs.scheduledFlightDuration,
    legs.cancelled,
    legs.aircraftTypeCode,
    legs.departure_delay_minutes,
    legs.arrival_delay_minutes,
    delay_agg.delayCode,
    delay_agg.delayDurationMinutes
from legs
left join delay_agg on legs.legId = delay_agg.flightLegId
