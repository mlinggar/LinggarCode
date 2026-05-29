{{ config(materialized='table', schema='gold') }}

with osm_data as (
    select * from {{ ref('stg_osm') }}
),

routes_data as (
    select * from {{ ref('dim_routes') }}
),

-- Spatial proximity match combining Swedish Grid roads and GPS points transformed to Grid
spatial_match as (
    select
        o.osm_id,
        o.targeted_region,
        o.asset_type,
        o.asset_name,
        o.asset_maxspeed,
        o.latitude,
        o.longitude,
        r.route_key,
        
        -- Measure distance in flat Swedish projection meters (3006)
        st_distance(
            st_geomfromwkt(r.route_geometry_wkt, 3006),
            st_transform(st_geomfromwkt('POINT(' || o.longitude || ' ' || o.latitude || ')', 4326), 3006)
        ) as distance_meters
        
    from osm_data o
    left join routes_data r
        on st_distance(
            st_geomfromwkt(r.route_geometry_wkt, 3006),
            st_transform(st_geomfromwkt('POINT(' || o.longitude || ' ' || o.latitude || ')', 4326), 3006)
        ) <= 200 -- Match if within 200 meters
        
    -- Keep only the absolute closest road segment for each camera
    qualify row_number() over (partition by o.osm_id order by distance_meters asc nulls last) = 1
)

select
    md5(osm_id::string) as asset_key,
    coalesce(route_key, md5('Unmapped')) as route_key,
    osm_id,
    targeted_region,
    asset_type,
    asset_name,
    asset_maxspeed,
    latitude,
    longitude,
    'POINT(' || longitude || ' ' || latitude || ')' as asset_geometry_wkt
from spatial_match