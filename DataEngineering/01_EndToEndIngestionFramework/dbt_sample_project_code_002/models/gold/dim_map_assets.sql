{{ config(materialized='view') }}

SELECT 
    node_id,
    ST_Y(asset_geo) AS latitude,  -- ST_Y extracts the Latitude from a geographic point
    ST_X(asset_geo) AS longitude, -- ST_X extracts the Longitude from a geographic point
    asset_type,
    operator,
    CONCAT(UPPER(SUBSTR(asset_type, 1, 1)), LOWER(SUBSTR(asset_type, 2)), ' (', COALESCE(operator, 'Unknown'), ')') AS asset_label
FROM DEV_SILVER.OPENSTREETMAP.SILVER_OPENSTREETMAP
WHERE asset_geo IS NOT NULL