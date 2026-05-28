{{ config(
    materialized='view',
    schema='gold',
    description='A live, flattened view of the most recent traffic telemetry for Tableau map rendering.'
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
    select * from {{ ref('dim_osm_regions') }}
),

joined_latest_state as (
    select 
        -- 1. Route Identity & Geospatial Data (For Tableau Map Plotting)
        r.route_id,
        r.route_name,
        r.route_geometry_wkt,
        
        -- 2. Live Telemetry Metrics (For Map Colors and Labels)
        f.observation_timestamp as last_updated_at,
        f.current_speed_kmh,
        f.delay_seconds,
        f.traffic_status,
        
        -- 3. Live Environmental Context (For Tooltips)
        w.weather_condition,
        w.road_surface_implication,
        f.temperature_celsius,
        
        -- 4. OSM Map Health (To ensure data reliability)
        o.targeted_region,
        o.provider_health_status
        
    from fact_traffic f
    left join dim_routes r 
        on f.route_key = r.route_key
    left join dim_weather w 
        on f.weather_condition_key = w.weather_condition_key
    left join dim_osm o 
        on f.region_key = o.region_key
)

select *
from joined_latest_state
-- CRITICAL MAP LOGIC: 
-- Partition by the route and sort by time so we only output the absolute newest record per road.
qualify row_number() over (partition by route_id order by last_updated_at desc) = 1