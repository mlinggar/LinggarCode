-- 1. Create the functional roles
USE ROLE USERADMIN;

CREATE ROLE data_engineer;
CREATE ROLE analytics_engineer;
CREATE ROLE bi_analyst;

-- 2. Grant roles to SYSADMIN so Account Admins can manage them
USE ROLE SECURITYADMIN;

GRANT ROLE data_engineer TO ROLE SYSADMIN;
GRANT ROLE analytics_engineer TO ROLE SYSADMIN;
GRANT ROLE bi_analyst TO ROLE SYSADMIN;

-- 3. Grant Basic Access to enter Database
USE ROLE SECURITYADMIN;

GRANT USAGE ON DATABASE traffic_db TO ROLE data_engineer;
GRANT USAGE ON DATABASE traffic_db TO ROLE analytics_engineer;
GRANT USAGE ON DATABASE traffic_db TO ROLE bi_analyst;

-- 4. Grant Data Engineer Raw Schema for Azure Snowpipe
GRANT USAGE ON SCHEMA traffic_db.raw TO ROLE data_engineer;
GRANT INSERT, SELECT ON ALL TABLES IN SCHEMA traffic_db.raw TO ROLE data_engineer;

-- Ensure Data Engineer can insert into any future tables created in RAW
GRANT INSERT, SELECT ON FUTURE TABLES IN SCHEMA traffic_db.raw TO ROLE data_engineer;


-- 5. Grant Schema Access for Analytics Engineer (dbt)

-- Read from RAW
GRANT USAGE ON SCHEMA traffic_db.raw TO ROLE analytics_engineer;
GRANT SELECT ON ALL TABLES IN SCHEMA traffic_db.raw TO ROLE analytics_engineer;

-- Ensure dbt can read future tables loaded by the Data Engineer
GRANT SELECT ON FUTURE TABLES IN SCHEMA traffic_db.raw TO ROLE analytics_engineer;

-- Full power over Bronze, Silver, Gold
GRANT ALL PRIVILEGES ON SCHEMA traffic_db.traffic_data_bronze TO ROLE analytics_engineer;
GRANT ALL PRIVILEGES ON SCHEMA traffic_db.traffic_data_silver TO ROLE analytics_engineer;
GRANT ALL PRIVILEGES ON SCHEMA traffic_db.traffic_data_gold TO ROLE analytics_engineer;


-- 6. Grant BI Analyst for Gold Schema 
-- Only allowed to read the GOLD schema
GRANT USAGE ON SCHEMA traffic_db.traffic_data_gold TO ROLE bi_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA traffic_db.traffic_data_gold TO ROLE bi_analyst;
GRANT SELECT ON ALL VIEWS IN SCHEMA traffic_db.traffic_data_gold TO ROLE bi_analyst;

-- Ensure BI Analyst gets read access to any NEW tables/views dbt creates in the future
GRANT SELECT ON FUTURE TABLES IN SCHEMA traffic_db.traffic_data_gold TO ROLE bi_analyst;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA traffic_db.traffic_data_gold TO ROLE bi_analyst;