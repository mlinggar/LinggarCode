import os
import json
import logging
import requests
from datetime import datetime, timezone
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

class OpenWeatherMapIngestor:
    def __init__(self):
        self.api_key = "ed681dbabc08fccd2fadf541aa545597"  
        # Hard-scoped strictly to Stockholm to align with the highway visualization maps
        self.cities = ["Stockholm"]
        self.account_url = "https://sttrafficswedendev001.blob.core.windows.net/raw"
        self.container_name = "raw"

    def execute_source_to_raw(self):
        logging.info("Authenticating token credentials via DefaultAzureCredential...")
        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(account_url=self.account_url, credential=credential)
        current_time = datetime.now(timezone.utc)
        
        for city in self.cities:
            logging.info(f"Triggering targeted OpenWeatherMap fetch for: {city}")
            url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&units=metric&appid={self.api_key}"
            
            try:
                response = requests.get(url, timeout=10)
                if response.status_code != 200:
                    logging.error(f"Failed to fetch weather for {city}: {response.status_code} - {response.text}")
                    continue
                    
                data = response.json()
                data["extracted_at_utc"] = current_time.isoformat()
                
                blob_path = (
                    f"weather/city={city.lower()}/"
                    f"year={current_time.year}/"
                    f"month={current_time.month:02d}/"
                    f"day={current_time.day:02d}/"
                    f"{city.lower()}_weather_{current_time.strftime('%H%M%S')}.json"
                )
                
                blob_client = blob_service_client.get_blob_client(container=self.container_name, blob=blob_path)
                blob_client.upload_blob(json.dumps(data, indent=4), overwrite=True)
                logging.info(f"Weather matrix safely written to Azure Data Lake for {city}.")
            except Exception as e:
                logging.error(f"An unexpected error occurred while processing weather for {city}: {str(e)}")

if __name__ == "__main__":
    pipeline = OpenWeatherMapIngestor()
    pipeline.execute_source_to_raw()