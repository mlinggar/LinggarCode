{{ config(materialized='table', schema='gold') }}

select distinct
    weather_condition_key,
    weather_condition,
    road_surface_implication
from {{ ref('int_weather_cleansed') }}

union all

select
    md5('Unknown'), 'Unknown', 'Normal'