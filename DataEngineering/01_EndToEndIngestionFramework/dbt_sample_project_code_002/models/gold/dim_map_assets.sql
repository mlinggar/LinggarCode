{{ config(materialized='view') }}

-- Part 1: Extract all real cameras and assets from OpenStreetMap
WITH osm_assets AS (
    SELECT 
        node_id,
        ST_Y(asset_geo) AS latitude,  
        ST_X(asset_geo) AS longitude, 
        asset_type,
        operator,
        CONCAT(UPPER(SUBSTR(asset_type, 1, 1)), LOWER(SUBSTR(asset_type, 2)), ' (', COALESCE(operator, 'Unknown'), ')') AS asset_label
    FROM DEV_SILVER.OPENSTREETMAP.SILVER_OPENSTREETMAP
    WHERE asset_geo IS NOT NULL
),

-- Part 2: Extract all the new virtual coordinates (Now using clean lowercase names!)
virtual_assets AS (
    SELECT 
        node_id,       
        latitude,      
        longitude,     
        'Virtual Sensor' AS asset_type,
        'System' AS operator,
        asset_label    
    FROM {{ ref('dim_map_assets_virtual_additions') }}
)

-- Combine both datasets cleanly together
SELECT * FROM osm_assets
UNION ALL
SELECT * FROM virtual_assets