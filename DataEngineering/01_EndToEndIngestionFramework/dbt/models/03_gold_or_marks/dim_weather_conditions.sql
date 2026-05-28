select distinct
    md5(weather_condition) as weather_condition_key,
    weather_condition,
    road_surface_implication
from {{ ref('int_weather_cleansed') }}