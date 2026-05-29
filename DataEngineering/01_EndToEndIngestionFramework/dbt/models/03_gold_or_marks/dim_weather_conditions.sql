{{ config(materialized='table', schema='gold') }}

select distinct
    md5(weather_condition) as weather_condition_key,
    weather_condition,
    road_surface_implication
from {{ ref('stg_weather') }}

union all

-- Bulletproof fallback row for delayed telemetry joins
select
    md5('Unknown') as weather_condition_key,
    'Unknown'      as weather_condition,
    'Normal'       as road_surface_implication