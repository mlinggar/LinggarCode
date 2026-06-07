-- 1. Create the functional roles
USE ROLE USERADMIN;

CREATE ROLE data_loader_role;
CREATE ROLE dbt_transformer_role;
CREATE ROLE bi_analyst_role;
CREATE ROLE data_engineer_role;

-- 2. Grand Data Engineer all role
USE ROLE SECURITYADMIN;

GRANT ROLE data_loader_role TO ROLE data_engineer_role;
GRANT ROLE dbt_transformer_role TO ROLE data_engineer_role;
GRANT ROLE bi_analyst_role TO ROLE data_engineer_role;

-- Assign highest custom role to SYSADMIN so Account Admins can manage it
GRANT ROLE data_engineer_role TO ROLE SYSADMIN;

-- 3. Grant Basic Access to enter Database
USE ROLE SECURITYADMIN;

GRANT USAGE ON DATABASE traffic_db TO ROLE data_loader_role;
GRANT USAGE ON DATABASE traffic_db TO ROLE dbt_transformer_role;
GRANT USAGE ON DATABASE traffic_db TO ROLE bi_analyst_role;


-- 4. Grant Data Loader Raw Schema for Azure Snowpipe
GRANT USAGE ON SCHEMA traffic_db.raw TO ROLE data_loader_role;
GRANT INSERT, SELECT ON ALL TABLES IN SCHEMA traffic_db.raw TO ROLE data_loader_role;

-- 5. Grant All Schema for dbt Transformer

-- Read from RAW
GRANT USAGE ON SCHEMA traffic_db.raw TO ROLE dbt_transformer_role;
GRANT SELECT ON ALL TABLES IN SCHEMA traffic_db.raw TO ROLE dbt_transformer_role;

-- Full power over Bronze, Silver, Gold
GRANT ALL PRIVILEGES ON SCHEMA traffic_db.traffic_data_bronze TO ROLE dbt_transformer_role;
GRANT ALL PRIVILEGES ON SCHEMA traffic_db.traffic_data_silver TO ROLE dbt_transformer_role;
GRANT ALL PRIVILEGES ON SCHEMA traffic_db.traffic_data_gold TO ROLE dbt_transformer_role;


-- 6. Grant BI Analyst for Gold Schema 
-- Only allowed to read only the GOLD schema
GRANT USAGE ON SCHEMA traffic_db.traffic_data_gold TO ROLE bi_analyst_role;
GRANT SELECT ON ALL TABLES IN SCHEMA traffic_db.traffic_data_gold TO ROLE bi_analyst_role;
GRANT SELECT ON ALL VIEWS IN SCHEMA traffic_db.traffic_data_gold TO ROLE bi_analyst_role;

-- Ensure to get read access to any NEW tables dbt creates in the future
GRANT SELECT ON FUTURE TABLES IN SCHEMA traffic_db.traffic_data_gold TO ROLE bi_analyst_role;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA traffic_db.traffic_data_gold TO ROLE bi_analyst_role;