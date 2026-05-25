{{ config(materialized='view') }}

-- Part 1: Extract real camera assets from OpenStreetMap
WITH osm_assets AS (
    SELECT 
        -- Bulletproof relationship key handling nulls and casing
        UPPER(TRIM(COALESCE(node_id::VARCHAR, ''))) AS node_id,
        COALESCE(ST_Y(asset_geo), 0.00)::FLOAT AS latitude,  
        COALESCE(ST_X(asset_geo), 0.00)::FLOAT AS longitude, 
        COALESCE(asset_type, 'Unknown')::VARCHAR AS asset_type,
        COALESCE(operator, 'Unknown')::VARCHAR AS operator,
        CONCAT(
            UPPER(SUBSTR(COALESCE(asset_type, 'Unknown'), 1, 1)), 
            LOWER(SUBSTR(COALESCE(asset_type, 'Unknown'), 2)), 
            ' (', 
            COALESCE(operator, 'Unknown'), 
            ')'
        )::VARCHAR AS asset_label
    FROM DEV_SILVER.OPENSTREETMAP.SILVER_OPENSTREETMAP
    WHERE asset_geo IS NOT NULL 
      AND node_id IS NOT NULL
),

-- Part 2: Extract our custom virtual coordinates from the seed file
virtual_assets AS (
    SELECT 
        UPPER(TRIM(COALESCE(node_id::VARCHAR, ''))) AS node_id, 
        COALESCE(latitude::FLOAT, 0.00) AS latitude,      
        COALESCE(longitude::FLOAT, 0.00) AS longitude,     
        'Virtual Sensor'::VARCHAR AS asset_type,
        'System'::VARCHAR AS operator,
        COALESCE(asset_label::VARCHAR, 'Virtual Sensor (System)') AS asset_label    
    FROM {{ ref('dim_map_assets_virtual_additions') }}
    WHERE node_id IS NOT NULL
)

SELECT * FROM osm_assets
UNION ALL
SELECT * FROM virtual_assets