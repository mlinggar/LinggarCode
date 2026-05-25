{{ config(materialized='view') }}

WITH ranked_traffic AS (
    SELECT 
        route_id, 
        measure_time,
        speed,
        travel_time,
        traffic_status,
        2673730 AS city_id, 
        ROW_NUMBER() OVER (PARTITION BY route_id ORDER BY measure_time DESC) as rn
    FROM DEV_SILVER.TRAFIKVERKET.SILVER_TRAFIKVERKET
)

SELECT 
    route_id::VARCHAR AS route_id, -- Safely convert to a Text String here instead!      
    city_id,        
    measure_time AS last_updated,
    speed,
    travel_time,
    traffic_status,
    CASE 
        WHEN traffic_status = 'freeflow' THEN 'Green'
        WHEN traffic_status = 'heavy' THEN 'Red'
        WHEN traffic_status = 'sluggish' THEN 'Yellow'
        ELSE 'Gray'
    END AS status_color
FROM ranked_traffic
WHERE rn = 1