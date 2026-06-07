
-- 1. SET UP WAREHOUSE SUSPEND AND RESUME

ALTER WAREHOUSE COMPUTE_WH SET 
    AUTO_SUSPEND = 60, 
    AUTO_RESUME = TRUE;



-- 2. CORE DATABASE, SCHEMA & FILE FORMAT SETUP

CREATE DATABASE IF NOT EXISTS traffic_db;
CREATE SCHEMA IF NOT EXISTS traffic_db.raw;
USE SCHEMA traffic_db.raw;

-- Shared high-performance JSON format
CREATE OR REPLACE FILE FORMAT traffic_db.raw.format_json_raw
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    IGNORE_UTF8_ERRORS = TRUE;



-- 3. EXTERNAL STAGE CONFIGURATION

-- The first /raw/ is the Azure Container. The second /raw/ is the folder.
CREATE OR REPLACE STAGE traffic_db.raw.azure_raw_stage
    URL = 'azure://sttrafficswedendev001.blob.core.windows.net/raw/raw/'
    STORAGE_INTEGRATION = azure_traffic_raw_int
    FILE_FORMAT = traffic_db.raw.format_json_raw;



-- 4. TARGET RAW APPEND-ONLY TABLES

CREATE OR REPLACE TABLE traffic_db.raw.raw_weather (
    json_data VARIANT,
    file_path STRING,
    ingested_at TIMESTAMP_NTZ DEFAULT CONVERT_TIMEZONE('Europe/Stockholm', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ 
);

CREATE OR REPLACE TABLE traffic_db.raw.raw_trafikverket (
    json_data VARIANT,
    file_path STRING,
    ingested_at TIMESTAMP_NTZ DEFAULT CONVERT_TIMEZONE('Europe/Stockholm', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE traffic_db.raw.raw_osm (
    json_data VARIANT,
    file_path STRING,
    ingested_at TIMESTAMP_NTZ DEFAULT CONVERT_TIMEZONE('Europe/Stockholm', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ
);


-- 6. DEPLOY AUTOMATED AUTO-INGEST SNOWPIPES


--- a. Pipe for OpenWeather Data (/raw/raw/weather/)
CREATE OR REPLACE PIPE traffic_db.raw.pipe_weather
    AUTO_INGEST = TRUE
    INTEGRATION = 'AZURE_TRAFFIC_QUEUE_INT'
    ERROR_INTEGRATION = azure_snowpipe_error_out_int
AS
COPY INTO traffic_db.raw.raw_weather (json_data, file_path)
FROM (
    SELECT $1, METADATA$FILENAME 
    FROM @traffic_db.raw.azure_raw_stage/weather/
);

--- b. Pipe for Trafikverket Data (/raw/raw/road/)
CREATE OR REPLACE PIPE traffic_db.raw.pipe_trafikverket
    AUTO_INGEST = TRUE
    INTEGRATION = 'AZURE_TRAFFIC_QUEUE_INT'
    ERROR_INTEGRATION = azure_snowpipe_error_out_int
AS
COPY INTO traffic_db.raw.raw_trafikverket (json_data, file_path)
FROM (
    SELECT $1, METADATA$FILENAME 
    FROM @traffic_db.raw.azure_raw_stage/road/
);

--- c. Pipe for OpenStreetMap Data (/raw/raw/osm/)
CREATE OR REPLACE PIPE traffic_db.raw.pipe_osm
    AUTO_INGEST = TRUE
    INTEGRATION = 'AZURE_TRAFFIC_QUEUE_INT'
    ERROR_INTEGRATION = azure_snowpipe_error_out_int
AS
COPY INTO traffic_db.raw.raw_osm (json_data, file_path)
FROM (
    SELECT $1, METADATA$FILENAME 
    FROM @traffic_db.raw.azure_raw_stage/osm/
);



-- 7. KICKSTART PIPES & LOAD PRE-EXISTING DATA
-- Forces Snowpipes to inspect exact paths right now and load existing files
ALTER PIPE traffic_db.raw.pipe_weather REFRESH;
ALTER PIPE traffic_db.raw.pipe_trafikverket REFRESH;
ALTER PIPE traffic_db.raw.pipe_osm REFRESH;



-- 8. PRODUCTION VERIFICATION & QUALITY CHECKS

-- Verify if Snowflake can actually see the files in nested 'raw/raw/' folders:
LIST @traffic_db.raw.azure_raw_stage/weather/;
LIST @traffic_db.raw.azure_raw_stage/road/;
LIST @traffic_db.raw.azure_raw_stage/osm/;

-- Check table counts if all the data has loaded:
SELECT COUNT(*) AS weather_total FROM traffic_db.raw.raw_weather;
SELECT COUNT(*) AS trafikverket_total FROM traffic_db.raw.raw_trafikverket;
SELECT COUNT(*) AS osm_total FROM traffic_db.raw.raw_osm;
