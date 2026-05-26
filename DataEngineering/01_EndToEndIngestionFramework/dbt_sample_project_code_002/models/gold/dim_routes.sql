{{ config(materialized='view') }}

SELECT 
    -- PRIMARY KEY
    MD5(UPPER(TRIM(route_id))) AS route_key,
    route_id,
    
    -- SPATIAL & DESCRIPTIVE ATTRIBUTES
    route_geo,
    route_name,
    country_code,
    
    CASE 
        WHEN LOWER(route_name) LIKE '%göteborg%' OR LOWER(route_name) LIKE '%gothenburg%' THEN 'Gothenburg Region'
        WHEN LOWER(route_name) LIKE '%skåne%' OR LOWER(route_name) LIKE '%malmö%' THEN 'Skåne Region'
        ELSE 'Stockholm Region'
    END AS regional_division

FROM {{ source('trafikverket', 'silver_trafikverket') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY route_id ORDER BY measure_time DESC) = 1