
  
    

  create  table "postgres"."silver_mart"."dim_airlines__dbt_tmp"
  
  
    as
  
  (
    -- mart.dim_airlines
-- Dimension dégénérée : liste distincte des compagnies présentes dans les données.
-- En l'état, n'apporte pas d'attributs enrichissants au-delà du code (déjà dans fct_flight_legs).
-- À enrichir avec : nom complet, pays, alliance (Star/SkyTeam/Oneworld), low-cost flag.
-- Grain : 1 ligne par airline_code.

select distinct
    airline_code as airline_key,
    airline_name
from "postgres"."silver_int"."flight_data__int_legs_ready"
where airline_code is not null
  );
  