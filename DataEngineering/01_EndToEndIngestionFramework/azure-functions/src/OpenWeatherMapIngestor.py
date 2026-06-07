import os
import json
import logging
import requests
from datetime import datetime, timezone
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

# --- This module is responsible for ingesting current weather data from OpenWeatherMap's API, specifically targeting the city of Stockholm to align with the geospatial assets from OpenStreetMap. ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# The OpenWeatherMapIngestor class encapsulates the logic for fetching current weather data for Stockholm, enriching it with metadata, and uploading it to Azure Blob Storage in a structured format for downstream processing and analysis.
class OpenWeatherMapIngestor:
    # The constructor initializes the API key for OpenWeatherMap, defines the target city for weather data retrieval, and sets up the Azure Blob Storage connection details, ensuring that the ingestor is configured to operate within the defined scope of the project.
    def __init__(self):
        self.api_key = "ed681dbabc08fccd2fadf541aa545597"  
        # Hard-scoped strictly to Stockholm to align with the highway visualization maps
        self.cities = ["Stockholm"]
        self.account_url = "https://sttrafficswedendev001.blob.core.windows.net/raw"
        self.container_name = "raw"

    # The execute_source_to_raw method iterates over the defined list of cities (in this case, just Stockholm), makes API calls to OpenWeatherMap to fetch current weather data, enriches the response with metadata, and uploads the resulting JSON data to Azure Blob Storage in a structured path format that organizes the data by city and date for efficient retrieval and management. It also includes robust error handling to log any issues encountered during the API calls or data processing steps.
    def execute_source_to_raw(self):
        logging.info("Authenticating token credentials via DefaultAzureCredential...")
        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(account_url=self.account_url, credential=credential)
        current_time = datetime.now(timezone.utc)
        
        # Iterate over the list of cities (currently only Stockholm) to fetch weather data, enrich it with metadata, and upload it to Azure Blob Storage in a structured format.
        for city in self.cities:
            logging.info(f"Triggering targeted OpenWeatherMap fetch for: {city}")
            url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&units=metric&appid={self.api_key}"
            
            # Implement robust error handling to catch and log any issues during the API call or data processing, ensuring that the pipeline can continue processing other cities (if added in the future) without interruption.
            try:
                response = requests.get(url, timeout=10)
                if response.status_code != 200:
                    logging.error(f"Failed to fetch weather for {city}: {response.status_code} - {response.text}")
                    continue
                    
                data = response.json()
                data["extracted_at_utc"] = current_time.isoformat()
                
                # Construct a blob path that organizes the ingested weather data by city and date, following a consistent naming convention for easy retrieval and management in Azure Blob Storage.
                blob_path = (
                    f"weather/city={city.lower()}/"
                    f"year={current_time.year}/"
                    f"month={current_time.month:02d}/"
                    f"day={current_time.day:02d}/"
                    f"{city.lower()}_weather_{current_time.strftime('%H%M%S')}.json"
                )
                
                # Log the connection attempt to Azure Blob Storage and upload the enriched weather data as a JSON file, ensuring that any existing file with the same name is overwritten to maintain the latest dataset for the city.
                blob_client = blob_service_client.get_blob_client(container=self.container_name, blob=blob_path)
                blob_client.upload_blob(json.dumps(data, indent=4), overwrite=True)
                logging.info(f"Weather matrix safely written to Azure Data Lake for {city}.")
            except Exception as e:
                logging.error(f"An unexpected error occurred while processing weather for {city}: {str(e)}")

# The main block allows for standalone execution of the OpenWeatherMapIngestor, enabling testing and debugging outside of the Azure Functions environment, while still adhering to the same data retrieval and processing logic defined in the execute_source_to_raw method.
if __name__ == "__main__":
    pipeline = OpenWeatherMapIngestor()
    pipeline.execute_source_to_raw()