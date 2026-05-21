import os
import logging
import requests
import json
from datetime import datetime
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

class OpenWeatherMapIngestor:
    def __init__(self):
        # Configuration setup
        self.api_key = "ed681dbabc08fccd2fadf541aa545597"  
        self.city = "Stockholm"
        self.url = f"https://api.openweathermap.org/data/2.5/weather?q={self.city}&units=metric&appid={self.api_key}"
        
        # Azure Storage ADLS Gen2 target properties
        self.account_name = "sttrafficswedendev001"
        self.container_name = "raw"
        self.account_url = f"https://sttrafficswedendev001.blob.core.windows.net/raw"

    def execute_source_to_raw(self):
        logging.info(f"Triggering active OpenWeatherMap fetch for: {self.city}")
        
        response = requests.get(self.url)
        if response.status_code != 200:
            raise Exception(f"OpenWeatherMap API returned error status: {response.status_code} - {response.text}")
            
        data = response.json()
        data["extracted_at_utc"] = datetime.utcnow().isoformat()
        
        current_time = datetime.utcnow()
        blob_path = f"weather/{current_time.year}/{current_time.month:02d}/{current_time.day:02d}/stockholm_weather_{current_time.strftime('%H%M%S')}.json"
        
        logging.info(f"Authenticating token credentials to map write pathway onto: {blob_path}")
        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(account_url=self.account_url, credential=credential)
        blob_client = blob_service_client.get_blob_client(container=self.container_name, blob=blob_path)
        
        blob_client.upload_blob(json.dumps(data, indent=4), overwrite=True)
        logging.info("OpenWeatherMap data ingestion processing complete.")

# This tells Python to execute the code ONLY when run directly via the terminal
if __name__ == "__main__":
    import sys
    # This allows local execution to find the 'src' directory properly
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    
    logging.basicConfig(level=logging.INFO)
    print("Testing OpenWeatherMap Ingestor locally...")
    
    try:
        ingestor = OpenWeatherMapIngestor()
        ingestor.execute_source_to_raw()
        print("Success! Check your Azure Storage Raw container.")
    except Exception as e:
        print(f"Test Failed: {str(e)}")