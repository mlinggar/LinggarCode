{{ config(materialized='view') }}

SELECT DISTINCT
    route_id,
    route_name,
    country_code,
    CONCAT(route_name, ', ', UPPER(country_code)) AS powerbi_location_search
FROM DEV_SILVER.TRAFIKVERKET.SILVER_TRAFIKVERKET