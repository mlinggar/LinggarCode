{{ config(materialized='view') }}

SELECT 
    -- 1. PRIMARY KEY (Unique hash identifier for this fact record)
    MD5(CONCAT(UPPER(TRIM(route_id)), '_', CAST(measure_time AS VARCHAR))) AS traffic_fact_key,

    -- 2. FOREIGN KEYS (To link out to your dimension tables)
    MD5(UPPER(TRIM(route_id))) AS route_key,
    
    MD5(UPPER(TRIM(
        CASE 
            WHEN LOWER(route_name) LIKE '%göteborg%' OR LOWER(route_name) LIKE '%gothenburg%' THEN 'GOTHENBURG'
            WHEN LOWER(route_name) LIKE '%skåne%' OR LOWER(route_name) LIKE '%malmö%' THEN 'MALMO'
            ELSE 'STOCKHOLM'
        END
    ))) AS city_key,
    
    -- 3. DEGENERATE DIMENSIONS (Natural text strings kept for easy reference)
    route_id,
    CASE 
        WHEN LOWER(route_name) LIKE '%göteborg%' OR LOWER(route_name) LIKE '%gothenburg%' THEN 'GOTHENBURG'
        WHEN LOWER(route_name) LIKE '%skåne%' OR LOWER(route_name) LIKE '%malmö%' THEN 'MALMO'
        ELSE 'STOCKHOLM'
    END AS city_name,
    
    -- 4. FACT METRICS
    speed AS live_speed_kmh,
    travel_time AS travel_time_seconds,
    traffic_status,
    
    CASE 
        WHEN LOWER(traffic_status) LIKE '%free%' OR LOWER(traffic_status) LIKE '%normal%' THEN 'Green'
        WHEN LOWER(traffic_status) LIKE '%heavy%' OR LOWER(traffic_status) LIKE '%slow%' THEN 'Yellow'
        WHEN LOWER(traffic_status) LIKE '%congested%' OR LOWER(traffic_status) LIKE '%blocked%' THEN 'Red'
        ELSE 'Gray'
    END AS status_color,
    
    -- 5. TIMESTAMPS
    TO_VARCHAR(measure_time, 'YYYY-MM-DD HH24:MI:SS') AS metrics_measured_at,
    load_timestamp AS dbt_loaded_at

FROM {{ source('trafikverket', 'silver_trafikverket') }}