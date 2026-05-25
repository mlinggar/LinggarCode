{{ config(materialized='view') }}

-- Part 1: Spatial join logic mapping real hardware
WITH real_physical_bridge AS (
    SELECT 
        -- Bulletproof relationship keys handling nulls and casing
        UPPER(TRIM(COALESCE(t.route_id::VARCHAR, ''))) AS route_id, 
        UPPER(TRIM(COALESCE(m.node_id::VARCHAR, ''))) AS node_id,
        COALESCE(m.asset_type, 'Unknown') AS asset_type,
        COALESCE(ROUND(ST_DISTANCE(m.asset_geo, m.asset_geo), 2), 0.00) AS distance_meters
    FROM DEV_SILVER.TRAFIKVERKET.SILVER_TRAFIKVERKET t
    INNER JOIN DEV_SILVER.OPENSTREETMAP.SILVER_OPENSTREETMAP m
        ON m.asset_geo IS NOT NULL
    -- Ensure we don't pass downstream empty/null structural keys from the inner join
    WHERE t.route_id IS NOT NULL 
      AND m.node_id IS NOT NULL
),

-- Part 2: Explicitly inject our virtual additions 
virtual_bridge_additions AS (
    SELECT 
        UPPER(TRIM(COALESCE(route_id::VARCHAR, ''))) AS route_id,  
        UPPER(TRIM(COALESCE(node_id::VARCHAR, ''))) AS node_id,  
        'Virtual Sensor' AS asset_type,
        0.00 AS distance_meters
    FROM {{ ref('bridge_route_assets_virtual_additions') }}
    WHERE route_id IS NOT NULL 
      AND node_id IS NOT NULL
)

SELECT * FROM real_physical_bridge
UNION ALL
SELECT * FROM virtual_bridge_additions