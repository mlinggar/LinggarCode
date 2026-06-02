{{ config(materialized='table', schema='gold') }}

select
    o.asset_key,
    o.osm_id,
    o.asset_name,
    o.asset_type,
    o.asset_maxspeed,
    o.latitude,
    o.longitude,
    -- Simple spatial match: Assign asset to closest route. (Requires Snowflake ST_DWITHIN / ST_DISTANCE)
    -- For simplicity, defaulting to an unmapped hash until spatial match is needed, or mapping via a cross join distance calc
    r.route_key
from {{ ref('int_osm_cleansed') }} o
left join {{ ref('int_routes_cleansed') }} r
    -- Convert Sweref99 points to Geography and match cameras to roads within 50 meters
    on st_dwithin(
        st_point(o.longitude, o.latitude), 
        to_geography(st_aswkt(st_transform(st_geomfromwkt(r.route_geometry_wkt, 3006), 4326))), 
        50
    )
qualify row_number() over (partition by o.asset_key order by r.route_key) = 1