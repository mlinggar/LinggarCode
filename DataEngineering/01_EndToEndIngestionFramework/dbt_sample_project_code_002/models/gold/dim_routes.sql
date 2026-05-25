{{ config(materialized='view') }}

SELECT DISTINCT
    -- Bulletproof relationship key handling casing, spacing, and nulls
    UPPER(TRIM(COALESCE(route_id::VARCHAR, ''))) AS route_id,
    
    -- Defend descriptive fields against nulls
    COALESCE(route_name, 'Unknown Route') AS route_name,
    COALESCE(country_code, 'SE') AS country_code,
    
    -- UPGRADE: Capture the native geometry object for spatial line segments
    route_geo,

    -- Dynamic regional categorization for dashboard filtering
    CASE 
        WHEN LOWER(route_name) LIKE '%göteborg%' OR LOWER(route_name) LIKE '%bäckebol%' THEN 'Gothenburg Region'
        WHEN LOWER(route_name) LIKE '%malmö%' OR LOWER(route_name) LIKE '%lund%' OR LOWER(route_name) LIKE '%kronetorp%' THEN 'Skåne Region'
        ELSE 'Stockholm Region'
    END AS regional_division,
    
    -- Bulletproof string concatenation ensuring no NULL propagation
    CONCAT(
        COALESCE(route_name, 'Unknown Route'), 
        ', ', 
        UPPER(TRIM(COALESCE(country_code, 'SE')))
    ) AS powerbi_location_search
FROM {{ source('trafikverket', 'silver_trafikverket') }}
-- Exclude rows completely missing a structural key 
WHERE route_id IS NOT NULL