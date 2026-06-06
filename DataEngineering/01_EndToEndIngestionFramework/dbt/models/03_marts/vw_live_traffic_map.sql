{{ config(
    materialized='view',
    schema='gold',
    description='A highly optimized, flattened view specifically tailored for Tableau map layers.'
) }}

with fact_traffic as (
    select * from {{ ref('fact_travel_times') }}
),
dim_routes as (
    select * from {{ ref('dim_routes') }}
),
dim_weather as (
    select * from {{ ref('dim_weather_conditions') }}
),
dim_osm as (
    select * from {{ ref('dim_osm_assets') }}
),

latest_traffic as (
    select 
        f.route_key,
        f.traffic_status,
        f.current_speed_kmh,
        f.observation_timestamp,
        f.weather_condition_key,
        f.temperature_celsius,
        f.asset_key
    from fact_traffic f
    qualify row_number() over (partition by f.route_key order by f.observation_timestamp desc) = 1
)

select 
    r.route_key,
    r.route_name as display_name,
    
    t.traffic_status,
    t.current_speed_kmh as live_speed_kmh,
    w.weather_condition as current_weather,
    t.temperature_celsius,
    t.observation_timestamp as last_updated_at,
    
    o.asset_name,
    o.asset_type,
    o.asset_maxspeed,
    
    to_geography(st_point(o.longitude, o.latitude)) as asset_map_point,
    to_geography(st_aswkt(st_transform(st_geomfromwkt(r.route_geometry_wkt, 3006), 4326))) as route_geometry

from dim_routes r
left join latest_traffic t 
    on r.route_key = t.route_key
left join dim_weather w 
    on t.weather_condition_key = w.weather_condition_key
left join dim_osm o 
    on t.asset_key = o.asset_key