-- 1. Create a secure Storage Integration
CREATE OR REPLACE STORAGE INTEGRATION azure_traffic_raw_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = 'd236863a-f43f-4500-9e6d-8a195ae609f8'
  STORAGE_ALLOWED_LOCATIONS = ('azure://sttrafficswedendev001.blob.core.windows.net/raw/');

-- COPY the 'AZURE_CONSENT_URL' from this output and open it in a browser to accept the integration.
DESC STORAGE INTEGRATION azure_traffic_raw_int;

-- 2. Hook Snowflake directly to your Azure Storage Queue
CREATE OR REPLACE NOTIFICATION INTEGRATION azure_traffic_queue_int
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = 'AZURE_STORAGE_QUEUE'
  ENABLED = TRUE
  AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://sttrafficswedendev001.queue.core.windows.net/snowflake-traffic-queue'
  AZURE_TENANT_ID = 'd236863a-f43f-4500-9e6d-8a195ae609f8';

-- COPY 'AZURE_MULTI_TENANT_APP_NAME' from this output and grant it "Storage Queue Data Receiver" in Azure IAM.
DESC NOTIFICATION INTEGRATION azure_traffic_queue_int;