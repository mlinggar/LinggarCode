{{ config(
    materialized='view',
    schema='gold',
    description='A unified traffic view enriched with nearby OpenStreetMap asset attributes via a horizontal left join.'
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

-- Get the latest live traffic status for each route segment
latest_traffic as (
    select 
        f.route_key,
        f.traffic_status,
        f.current_speed_kmh,
        f.observation_timestamp,
        f.weather_condition_key
    from fact_traffic f
    qualify row_number() over (partition by f.route_key order by f.observation_timestamp desc) = 1
)

select 
    -- Core Route Identifiers
    r.route_key,
    r.route_name             as display_name,
    
    -- Live Traffic & Weather Metrics
    t.traffic_status,
    t.current_speed_kmh      as live_speed_kmh,
    w.weather_condition      as current_weather,
    t.observation_timestamp  as last_updated_at,
    
    -- Enriched OSM Asset Data (Connected horizontally!)
    o.osm_id                 as asset_id,
    o.asset_name             as asset_name,
    o.asset_type             as asset_type,         -- No longer null! Contains 'speed_camera', etc.
    o.asset_maxspeed         as asset_maxspeed,
    
    -- Geographic Shapes
    o.longitude              as asset_longitude,
    o.latitude               as asset_latitude,
    st_aswkt(st_transform(st_geomfromwkt(r.route_geometry_wkt, 3006), 4326)) as route_geometry_wkt

from dim_routes r
left join latest_traffic t 
    on r.route_key = t.route_key
left join dim_weather w 
    on t.weather_condition_key = w.weather_condition_key
left join dim_osm o 
    on r.route_key = o.route_key  -- Bridges the asset directly to the road it belongs to