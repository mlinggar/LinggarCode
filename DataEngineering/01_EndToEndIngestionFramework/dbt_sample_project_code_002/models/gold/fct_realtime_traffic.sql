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
        
        -- FIXED: Dynamic reference grouping mapping back to our weather dimension labels
        CASE 
            WHEN LOWER(route_name) LIKE '%göteborg%' OR LOWER(route_name) LIKE '%bäckebol%' THEN 'GOTHENBURG'
            WHEN LOWER(route_name) LIKE '%malmö%' OR LOWER(route_name) LIKE '%lund%' OR LOWER(route_name) LIKE '%kronetorp%' THEN 'MALMO'
            ELSE 'STOCKHOLM'
        END AS associated_city, 
        
        -- Use the exact same bulletproof key transformation inside the window partition
        ROW_NUMBER() OVER (
            PARTITION BY UPPER(TRIM(COALESCE(route_id::VARCHAR, ''))) 
            ORDER BY measure_time DESC
        ) AS rn
    FROM {{ source('trafikverket', 'silver_trafikverket') }}
    -- Filter out rows missing vital relationship keys or sorting timestamps
    WHERE route_id IS NOT NULL 
      AND measure_time IS NOT NULL
)

SELECT 
    route_id,      
    associated_city AS city_name,        
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