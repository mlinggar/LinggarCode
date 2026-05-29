-- ==========================================
-- 1. CLEAN UP OLD PIPES
-- ==========================================
DROP PIPE IF EXISTS traffic_db.bronze.pipe_weather;
DROP PIPE IF EXISTS traffic_db.bronze.pipe_trafikverket;
DROP PIPE IF EXISTS traffic_db.bronze.pipe_osm;


-- ==========================================
-- 2. CORE DATABASE, SCHEMA & FILE FORMAT SETUP
-- ==========================================
CREATE DATABASE IF NOT EXISTS traffic_db;
CREATE SCHEMA IF NOT EXISTS traffic_db.bronze;
USE SCHEMA traffic_db.bronze;

-- Shared high-performance JSON format
CREATE OR REPLACE FILE FORMAT traffic_db.bronze.format_json_raw
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    IGNORE_UTF8_ERRORS = TRUE;


-- ==========================================
-- 3. EXTERNAL STAGE CONFIGURATION (FIXED PATH)
-- ==========================================
-- The first /raw/ is the Azure Container. The second /raw/ is your folder.
CREATE OR REPLACE STAGE traffic_db.bronze.azure_raw_stage
    URL = 'azure://sttrafficswedendev001.blob.core.windows.net/raw/raw/'
    STORAGE_INTEGRATION = azure_traffic_raw_int
    FILE_FORMAT = traffic_db.bronze.format_json_raw;


-- ==========================================
-- 4. TARGET RAW APPEND-ONLY TABLES
-- ==========================================
CREATE OR REPLACE TABLE traffic_db.bronze.raw_weather (
    json_data VARIANT,
    file_path STRING,
    ingested_at TIMESTAMP_NTZ DEFAULT CONVERT_TIMEZONE('Europe/Stockholm', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ 
);

CREATE OR REPLACE TABLE traffic_db.bronze.raw_trafikverket (
    json_data VARIANT,
    file_path STRING,
    ingested_at TIMESTAMP_NTZ DEFAULT CONVERT_TIMEZONE('Europe/Stockholm', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE traffic_db.bronze.raw_osm (
    json_data VARIANT,
    file_path STRING,
    ingested_at TIMESTAMP_NTZ DEFAULT CONVERT_TIMEZONE('Europe/Stockholm', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ
);


-- ==========================================
-- 5. DEPLOY AUTOMATED AUTO-INGEST SNOWPIPES
-- ==========================================

-- 1. Pipe for OpenWeather Data (looks in /raw/raw/weather/)
CREATE OR REPLACE PIPE traffic_db.bronze.pipe_weather
    AUTO_INGEST = TRUE
    INTEGRATION = 'AZURE_TRAFFIC_QUEUE_INT'
AS
COPY INTO traffic_db.bronze.raw_weather (json_data, file_path)
FROM (
    SELECT $1, METADATA$FILENAME 
    FROM @traffic_db.bronze.azure_raw_stage/weather/
);

-- 2. Pipe for Trafikverket Data (looks in /raw/raw/road/)
CREATE OR REPLACE PIPE traffic_db.bronze.pipe_trafikverket
    AUTO_INGEST = TRUE
    INTEGRATION = 'AZURE_TRAFFIC_QUEUE_INT'
AS
COPY INTO traffic_db.bronze.raw_trafikverket (json_data, file_path)
FROM (
    SELECT $1, METADATA$FILENAME 
    FROM @traffic_db.bronze.azure_raw_stage/road/
);

-- 3. Pipe for OpenStreetMap Data (looks in /raw/raw/osm/)
CREATE OR REPLACE PIPE traffic_db.bronze.pipe_osm
    AUTO_INGEST = TRUE
    INTEGRATION = 'AZURE_TRAFFIC_QUEUE_INT'
AS
COPY INTO traffic_db.bronze.raw_osm (json_data, file_path)
FROM (
    SELECT $1, METADATA$FILENAME 
    FROM @traffic_db.bronze.azure_raw_stage/osm/
);


-- ==========================================
-- 6. KICKSTART PIPES & LOAD PRE-EXISTING DATA
-- ==========================================
-- Forces Snowpipes to inspect your exact paths right now and load existing files
-- ALTER PIPE traffic_db.bronze.pipe_weather REFRESH;
-- ALTER PIPE traffic_db.bronze.pipe_trafikverket REFRESH;
-- ALTER PIPE traffic_db.bronze.pipe_osm REFRESH;


-- ==========================================
-- 7. PRODUCTION VERIFICATION & QUALITY CHECKS
-- ==========================================
-- First, verify Snowflake can actually see the files in your nested 'raw/raw/' folders:
LIST @traffic_db.bronze.azure_raw_stage/weather/;
LIST @traffic_db.bronze.azure_raw_stage/road/;
LIST @traffic_db.bronze.azure_raw_stage/osm/;

-- Wait about 10-15 seconds after the refresh, then check your table counts:
SELECT COUNT(*) AS weather_total FROM traffic_db.bronze.raw_weather;
SELECT COUNT(*) AS trafikverket_total FROM traffic_db.bronze.raw_trafikverket;
SELECT COUNT(*) AS osm_total FROM traffic_db.bronze.raw_osm;