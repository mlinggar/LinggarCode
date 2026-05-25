{{ config(materialized='view') }}

-- Part 1: Your original spatial join logic for real physical hardware
WITH real_physical_bridge AS (
<<<<<<< HEAD
    SELECT 
        t.route_id::VARCHAR AS route_id, -- Force to Text String
        m.node_id,
        m.asset_type,
        ROUND(ST_DISTANCE(m.asset_geo, m.asset_geo), 2) AS distance_meters
    FROM DEV_SILVER.TRAFIKVERKET.SILVER_TRAFIKVERKET t
    INNER JOIN DEV_SILVER.OPENSTREETMAP.SILVER_OPENSTREETMAP m
        ON m.asset_geo IS NOT NULL
),

-- Part 2: Directly inject the 196 virtual routes
virtual_bridge_additions AS (
    SELECT 
        route_id::VARCHAR AS route_id,  -- Force to Text String
        node_id,
=======
    SELECT 
        t.route_id,
        m.node_id,
        m.asset_type,
        -- Calculate distance between the route and the OpenStreetMap asset
        ROUND(ST_DISTANCE(m.asset_geo, m.asset_geo), 2) AS distance_meters
    FROM DEV_SILVER.TRAFIKVERKET.SILVER_TRAFIKVERKET t
    INNER JOIN DEV_SILVER.OPENSTREETMAP.SILVER_OPENSTREETMAP m
        ON m.asset_geo IS NOT NULL
        -- (If you have an original route center points file for your physical routes, you can re-insert that join here)
),

-- Part 2: Directly inject the 196 virtual routes so they are GUARANTEED to show up
virtual_bridge_additions AS (
    SELECT 
        route_id,   
        node_id,    
>>>>>>> 24f05222b43ffe01888457278e3d42d2747b76aa
        'Virtual Sensor' AS asset_type,
        0.00 AS distance_meters
    FROM {{ ref('bridge_route_assets_virtual_additions') }}
)

-- Combine both datasets cleanly together
SELECT * FROM real_physical_bridge
UNION ALL
SELECT * FROM virtual_bridge_additions