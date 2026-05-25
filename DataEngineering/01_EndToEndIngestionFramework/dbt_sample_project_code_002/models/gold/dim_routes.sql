{{ config(materialized='view') }}

SELECT DISTINCT
    -- Bulletproof relationship key handling casing, spacing, and nulls
    UPPER(TRIM(COALESCE(route_id::VARCHAR, ''))) AS route_id,
    
    -- Defend descriptive fields against nulls
    COALESCE(route_name, 'Unknown Route') AS route_name,
    COALESCE(country_code, 'SE') AS country_code,
    
    -- Bulletproof string concatenation ensuring no NULL propagation
    CONCAT(
        COALESCE(route_name, 'Unknown Route'), 
        ', ', 
        UPPER(TRIM(COALESCE(country_code, 'SE')))
    ) AS powerbi_location_search
FROM DEV_SILVER.TRAFIKVERKET.SILVER_TRAFIKVERKET
-- Exclude rows completely missing a structural key 
WHERE route_id IS NOT NULL