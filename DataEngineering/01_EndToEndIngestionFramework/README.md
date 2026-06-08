# Live Traffic & Weather Data Platform
### This Project is for Final Assessment of Data Engineering Course, Data Analyst Program (DA27)

## Executive Summary
This repository contains the infrastructure, transformation logic, and orchestration code for a near-real-time live traffic and weather data platform. Built on a modern data stack using Azure, Snowflake, and dbt, the platform processes streaming telemetry data, correlates it with live weather forecasts, and maps it to static physical highway assets for downstream BI consumption in Tableau.

A core focus of this project is highly optimized cloud architecture. By implementing strict Medallion data modeling, peak-hour smart orchestration, and aggressive warehouse compute optimization, this platform achieves sub-15-minute data latency during critical business hours while keeping cloud compute and orchestration costs at absolute minimums.

## Architecture & Tech Stack

### Technology Stack
* **Cloud Infrastructure & Storage:** Azure Blob Storage (Data Lake Gen2), Azure Event Grid
* **Data Ingestion:** Snowflake Snowpipe (Automated Event-Driven Loading)
* **Data Warehouse & Compute:** Snowflake
* **Data Transformation & Orchestration:** dbt Cloud (Data Build Tool)
* **BI & Analytics:** Tableau

### Data Sources
1. **Trafikverket API:** Live traffic events, highway telemetry, and vehicle speeds.
2. **OpenWeatherMap API:** Continuous weather forecasts and conditions.
3. **OpenStreetMap (OSM):** Static physical highway assets (speed cameras, toll booths, road geometries).

### Medallion Data Flow
* **Bronze (Staging):** Raw JSON payloads from Azure are ingested into Snowflake via Snowpipe. dbt staging views flatten the variant data and enforce initial explicit type-casting.
* **Silver (Intermediate):** Heavy cleansing, deduplication, JSON parsing, and standardization of traffic events and road surface implications.
* **Gold (Marts):** Enforced data contracts, dimensional modeling (Star Schema), spatial geometric matching, and final materialized views for Tableau.

## Key Engineering Achievements

### 1. Smart Orchestration & Zero-Cost Pipeline Tiering
To maintain near-real-time updates without exceeding the dbt Cloud Developer tier limits (3,000 monthly builds), the orchestration schedule is strictly decoupled using explicit dbt tagging and bi-modal peak-hour cron schedules. 

Instead of processing all data continuously, the pipeline executes based on data volatility and user demand:
* **Static Geography (`tag:static_geo`):** Built weekly, at 6 Am on Monday (20 builds/month).
* **Weather Conditions (`tag:weather`):** Built every 1 hour exclusively during morning and evening rush hours (396 builds/month), only weekday.
* **Live Traffic (`tag:live_traffic`):** Built every 15 minutes exclusively during morning (06:00-09:00) and evening (15:00-18:00) rush hours, only weekday (2,112 builds/month).
* **Result:** 2,528 total monthly builds. This delivers real-time commuter data and synchronized hourly weather changes while preserving a 130-build buffer for CI/CD development, keeping orchestration costs at $0.

### 2. Extreme Cloud Compute Optimization
By optimizing the Snowflake virtual warehouse settings and explicitly tagging dbt jobs to avoid unnecessary full-DAG rebuilds, the Total Cost of Ownership (TCO) for the data stack was reduced by **nearly 80%**. 

Adjusting the compute cluster to `AUTO_SUSPEND = 60` seconds ensured that the warehouse sleeps immediately after the dbt micro-batches complete, successfully dropping daily compute uptime from a constant 24 hours down to approximately 5 hours.

| Financial Metric | Before (Unoptimized Stack) | After (Optimized Stack) | Your Net Savings |
| :--- | :--- | :--- | :--- |
| **dbt Cloud Cost** | $100.00/month <br> *jobs run all day: 34,560 builds* | $0.00/month <br> *explicit tagging during peak hours: 2,528 builds* | $100.00 saved monthly |
| **Snowflake Compute** | $1,497.60/month <br> *warehouse working 24/7* | $34.65/month <br> *Auto-suspend 60s + stacked jobs* | $1,462.95 saved monthly |
| **Azure Ingestion & Storage** | $20.00/month | $20.00/month | $0.00 (Fixed Infrastructure) |
| **Total Monthly Spend** | **$1,617.60** | **$54.65** | **$1,562.95 saved / month** |
| **Total Yearly Spend** | **$19,411.20** | **$655.80** | **$18,755.40 saved / year** |

### 3. Strict Data Contracts & Quality Control
The platform implements a strict governance "firewall" at the Gold layer to automatically block schema drift and invalid records from reaching production dashboards.

Centralized inside Medallion-specific `schema.yml` files, the pipeline utilizes dbt model contracts (`contract: { enforced: true }`). If upstream API schemas change or data types mismatch (e.g., an integer becomes a string), the pipeline fails safely before compilation. Furthermore, automated tests (uniqueness, completeness, and referential integrity) are executed on every run.

### 4. Role-Based Access Control (RBAC) Architecture
Security and data access are managed via a strict functional RBAC hierarchy in Snowflake:
* `ANALYTICS_ENGINEER`: Use dbt labs. It needs to read from RAW, and have full power to create, update, and drop tables in BRONZE, SILVER, and GOLD
* `BI_ANALYST`: Use Tableau/ business users. It only needs read-only access, and only to the GOLD schema (shouldn't see raw or silver data).
* `DATA_ENGINEER`: Inherits all the roles above so can build, troubleshoot, and see everything including managing the Azure-Snowpipe.
  
### 5. GitHub Folder Explanation
GitHub is used for version control, tracking every change and detail across this project. The repository is structured as follows:
* `azure-function`: Dedicated to Azure Functions. All ingestion Python code is housed within the `src` folder.
* `data-factory`: Dedicated to Azure Data Factory pipelines, orchestration, and the Azure-Git connection.
* `dbt`: Dedicated to the dbt Git connection for data transformation layers.
* `snowflake-snowpipe-integration`: Dedicated to the Snowflake-Azure integration setup, Snowpipe auto-ingestion configurations, and RBAC (Role-Based Access Control) queries.

## Repository Structure

```text
├── dbt_project.yml
├── models/
│   ├── staging/
│   │   ├── stg_trafikverket.sql
│   │   ├── stg_weather.sql
│   │   ├── stg_osm.sql
│   │   └── schema.yml         # Bronze tags and source definitions
│   ├── intermediate/
│   │   ├── int_traffic_events_cleansed.sql
│   │   ├── int_weather_cleansed.sql
│   │   ├── int_routes_cleansed.sql
│   │   ├── int_osm_cleansed.sql
│   │   └── schema.yml         # Silver tags and intermediate metadata
│   ├── marts/
│   │   ├── dim_routes.sql
│   │   ├── dim_weather_conditions.sql
│   │   ├── dim_osm_assets.sql
│   │   ├── fact_travel_times.sql
│   │   ├── vw_live_traffic_map.sql
│   │   └── schema.yml         # Gold contracts, tests, and catalog descriptions
