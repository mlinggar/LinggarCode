{{ config(materialized='view') }}

WITH ranked_traffic AS (
    SELECT 
        -- Bulletproof relationship key handling casing and spacing consistently
        UPPER(TRIM(COALESCE(route_id::VARCHAR, ''))) AS route_id, 
        measure_time,
        
        -- Defend against null metrics with safe numeric fallbacks
        COALESCE(speed, 0) AS speed,
        COALESCE(travel_time, 0) AS travel_time,
        COALESCE(traffic_status, 'Unknown') AS traffic_status,
        
        2673730 AS city_id, 
        
        -- Use the exact same bulletproof key transformation inside the window partition
        ROW_NUMBER() OVER (
            PARTITION BY UPPER(TRIM(COALESCE(route_id::VARCHAR, ''))) 
            ORDER BY measure_time DESC
        ) AS rn
    FROM DEV_SILVER.TRAFIKVERKET.SILVER_TRAFIKVERKET
    -- Filter out rows missing vital relationship keys or sorting timestamps
    WHERE route_id IS NOT NULL 
      AND measure_time IS NOT NULL
)

SELECT 
    route_id,      
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