{{ config(materialized='view') }}

WITH map_assets AS (
    SELECT 
        node_id,
        asset_type,
        asset_geo 
    FROM DEV_SILVER.OPENSTREETMAP.SILVER_OPENSTREETMAP
    WHERE asset_geo IS NOT NULL
),

clean_routes AS (
    SELECT 
        t.route_id,
        t.route_name,
        -- Create a spatial point out of our manual CSV coordinates
        ST_GEOGRAPHYFROMWKT('POINT(' || p.longitude || ' ' || p.latitude || ')') AS route_geo
    FROM DEV_SILVER.TRAFIKVERKET.SILVER_TRAFIKVERKET t
    -- Join our real-time traffic data to our manual coordinate seed!
    INNER JOIN {{ ref('route_center_points') }} p
        ON t.route_name = p.route_name
)

SELECT 
    r.route_id,
    m.node_id,
    m.asset_type,
    ROUND(ST_DISTANCE(r.route_geo, m.asset_geo), 2) AS distance_meters
FROM clean_routes r
INNER JOIN map_assets m
    -- We can expand this to 1000 meters (1km) so the center point catches more cameras!
    ON ST_DWITHIN(r.route_geo, m.asset_geo, 1000)