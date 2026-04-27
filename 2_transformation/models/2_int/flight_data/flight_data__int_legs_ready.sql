-- int.flight_data__int_legs_ready
-- Modèle int final : agrège toutes les features nécessaires au ML en une seule ligne par leg.
-- Construit en trois étapes (CTEs chaînées) :
--   1. base          : reprend int_delays_leg + features temporelles (heure, jour, mois, jour du mois).
--   2. dep_congestion: joint la congestion de l'aéroport de départ pour ce jour.
--   3. arr_congestion: joint la congestion de l'aéroport d'arrivée pour ce jour.
-- is_delayed = true si l'un des trois indicateurs de retard atteint 15 minutes ou plus
--              (seuil standard IATA de retard significatif).
-- Grain : 1 ligne par leg. Alimentation directe de fct_flight_legs (mart).
{{ config(schema='int', materialized='view') }}

with base as (
    select
        d.legId,
        d.flightId,
        d.flightNumber,
        d.flightScheduleDate,
        d.airlineCode,
        d.departureAirportCode,
        d.arrivalAirportCode,
        d.scheduledDeparture,
        d.actualDeparture,
        d.scheduledArrival,
        d.actualArrival,
        d.cancelled,
        d.delayCode,
        d.delayDurationMinutes,
        d.departureDelayMinutes,
        d.arrivalDelayMinutes,
        d.scheduledFlightDuration,
        d.aircraftCode,
        {{ parse_iso8601_duration_minutes('d.scheduledFlightDuration') }} as scheduledFlightDuration,
        extract(dow from d.scheduledDeparture)::int as departureWeekday,
        extract(month from d.scheduledDeparture)::int as departureMonth,
        extract(hour from d.scheduledDeparture)::int as departureHour,
        extract(day from d.scheduledDeparture)::int as departureMonthday
    from {{ ref('flight_data__int_delays_leg') }} d
),
with_dep_congestion as (
    select
        b.*,
        coalesce(dep.nbFlightDeparting, 0) as nbFlightDepartingDepartureAirport,
        coalesce(dep.nbFlightArriving, 0) as nbFlightArrivingDepartureAirport
    from base b
    left join {{ ref('flight_data__int_airport_congestion') }} dep
        on b.departureAirportCode = dep.airportCode
        and b.flightScheduleDate = dep.flightScheduleDate
),
with_arr_congestion as (
    select
        w.*,
        coalesce(arr.nbFlightDeparting, 0) as nbFlightDepartingArrivalAirport,
        coalesce(arr.nbFlightArriving, 0) as nbFlightArrivingArrivalAirport
    from with_dep_congestion w
    left join {{ ref('flight_data__int_airport_congestion') }} arr
        on w.arrivalAirportCode = arr.airportCode
        and w.flightScheduleDate = arr.flightScheduleDate
),
with_delay_airport as (
    select 
        a.*,
        coalesce(dap.DepartureAirportDelayedShare,0) as DepartureAirportDelayedShare
    from with_arr_congestion a
    left join {{ref ("flight_data__int_airport_delays")}} dap
        on a.departureAirportCode = dap.departureAirportCode
        and a.flightScheduleDate = dap.flightScheduleDate
),
with_delay_aircraft as (
    select 
        wdap.*,
        coalesce(dac.aircraftDelayedShare,0) as aircraftDelayedShare
    from with_delay_airport wdap
    left join {{ref ("flight_data__int_aircraft_delays")}} dac
        on wdap.aircraftCode = dac.aircraftCode
        and wdap.flightScheduleDate = dac.flightScheduleDate
),
with_delay_airline as (
    select 
        wdac.*,
        coalesce(dap.airlineDelayedShare,0) as airlineDelayedShare
    from with_delay_aircraft wdac
    left join {{ref ("flight_data__int_airline_delays")}} dal
        on wdac.airlineCode = dal.airlineCode
        and wdac.flightScheduleDate = dal.flightScheduleDate
)
select
    legId,
    flightId,
    flightNumber,
    flightScheduleDate,
    airlineCode,
    departureAirportCode,
    arrivalAirportCode,
    scheduledDeparture,
    actualDeparture,
    scheduledArrival,
    actualArrival,
    cancelled,
    delayCode,
    delayDurationMinutes,
    departureDelayMinutes,
    arrivalDelayMinutes,
    scheduledFlightDuration,
    aircraftCode,
    departureWeekDay,
    departureMonth,
    departureHour,
    departureMonthDay,
    nbFlightDepartingDepartureAirport,
    nbFlightArrivingDepartureAirport,
    nbFlightDepartingArrivalAirport,
    nbFlightArrivingArrivalAirport,
    DepartureAirportDelayedShare,
    aircraftDelayedShare,
    airlineDelayedShare

    case
        when coalesce(departureDelayMinutes, 0) >= 15
          or coalesce(arrivalDelayMinutes, 0) >= 15
          or coalesce(delayDurationMinutes, 0) >= 15
        then true
        else false
    end as is_delayed
from with_delay_airline

