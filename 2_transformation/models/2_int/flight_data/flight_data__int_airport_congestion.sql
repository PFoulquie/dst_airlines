-- int.flight_data__int_airport_congestion
-- Feature ML : proxy de congestion aéroportuaire.
-- Calcule, pour chaque aéroport et chaque jour, le nombre de vols au départ et à l'arrivée.
-- Un aéroport très chargé ce jour-là est plus susceptible de générer des retards en cascade.
-- FULL OUTER JOIN departures/arrivals : capture les aéroports qui ne figurent que d'un seul côté.
-- Grain : 1 ligne par (airportCode, flightScheduleDate).
{{ config(schema='int', materialized='view') }}

with legs_with_date as (
    select
        l.departureAirportCode,
        l.arrivalAirportCode,
        f.flightScheduleDate
    from {{ ref('flight_data__source_operational_flight_legs') }} l
    join {{ ref('flight_data__source_operational_flights') }} f on l.flight_id = f.id
),
departures as (
    select
        departureAirportCode as airportCode,
        flightScheduleDate,
        count(*) as nbFlightDeparting
    from legs_with_date
    where departureAirportCode is not null
    group by departureAirportCode, flightScheduleDate
),
arrivals as (
    select
        arrivalAirportCode as airportCode,
        flightScheduleDate,
        count(*) as nb_arriving
    from legs_with_date
    where arrivalAirportCode is not null
    group by arrivalAirportCode, flightScheduleDate
)
select
    coalesce(d.airportCode, a.airportCode) as airportCode,
    coalesce(d.flightScheduleDate, a.flightScheduleDate) as flightScheduleDate,
    coalesce(d.nbFlightDeparting, 0) as nbFlightDeparting,
    coalesce(a.nbFlightArriving, 0) as nbFlightArriving
from departures d
full outer join arrivals a
    on d.airportCode = a.airportCode
    and d.flightScheduleDate = a.flightScheduleDate
