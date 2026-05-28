{{ config(materialized='view', tags=['map']) }}

with latest_traffic as (
    -- Get the absolute newest traffic status for each route directly from the fact view
    select * from {{ ref('fct_realtime_traffic') }}
    qualify row_number() over (partition by route_id order by measure_time desc, dbt_loaded_at desc) = 1
),

dim_routes as (
    select * from {{ ref('dim_routes') }}
),

dim_weather as (
    select * from {{ ref('dim_weather') }}
),

dim_osm as (
    select * from {{ ref('dim_openstreet') }}
)

select 
    -- FACT METADATA
    lt.traffic_fact_key,
    lt.route_id,
    r.route_name,
    r.regional_division,
    r.country_code,
    lt.city_name as associated_city,
    
    -- TRAFFIC METRICS
    lt.live_speed_kmh,
    lt.travel_time_seconds,
    lt.traffic_status,
    lt.status_color,
    lt.metrics_measured_at,
    
    -- WEATHER METRICS
    w.weather_condition,
    w.temp_celsius,
    w.wind_speed,

    -- OSM METADATA
    o.asset_type as nearby_infrastructure,
    o.operator as infrastructure_operator,

    -- GEOMETRIES (Kept as raw GEOGRAPHY objects so Tableau natively maps them)
    r.route_geo as geography_object,
    o.asset_geo as infrastructure_object

from latest_traffic lt  
inner join dim_routes r 
    on lt.route_key = r.route_key
left join dim_weather w 
    on lt.city_key = w.city_key
left join dim_osm o 
    -- Matches OSM points (like cameras) that are within 300 meters of the active highway
    on st_dwithin(r.route_geo, o.asset_geo, 300)