import os
import logging
import requests
import json
import time
from datetime import datetime
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

class OpenStreetMapIngestor:
    def __init__(self):
        # Switched back to the main, more powerful Overpass server now that our headers are fixed
        self.url = "https://overpass.openstreetmap.fr/api/interpreter"
        
        # Azure Storage ADLS Gen2 target properties
        self.account_name = "sttrafficswedendev001"
        self.container_name = "raw"
        self.account_url = "https://sttrafficswedendev001.blob.core.windows.net/raw"

    def execute_source_to_raw(self):
        logging.info("Formulating Overpass QL query for Stockholm highway assets...")
        
        # INCREASED TIMEOUT to 90 seconds to prevent 504 Gateway drops during peak load times
        overpass_query = """
        [out:json][timeout:90];
        (
          node["highway"="speed_camera"](59.2,17.8,59.5,18.3);
          node["highway"="toll_gantry"](59.2,17.8,59.5,18.3);
        );
        out body;
        """
        
        # Using a valid email bypasses the 406 firewall block
        headers = {
            "User-Agent": "NordicTrafficDataFramework/1.0 (linggar.pangestu@hyperisland.se)"
        }
        
        logging.info("Sending query to OpenStreetMap Overpass API. Waiting for response...")
        response = requests.post(self.url, data=overpass_query.encode('utf-8'), headers=headers)
        
        if response.status_code != 200:
            raise Exception(f"OpenStreetMap Overpass API returned an error: {response.status_code} - {response.text}")
            
        data = response.json()
        data["extracted_at_utc"] = datetime.utcnow().isoformat()
        
        current_time = datetime.utcnow()
        blob_path = f"osm/year={current_time.year}/month={current_time.month:02d}/day={current_time.day:02d}/stockholm_osm_assets_{current_time.strftime('%H%M%S')}.json"
        
        logging.info(f"Authenticating token credentials to stream map data onto: {blob_path}")
        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(account_url=self.account_url, credential=credential)
        blob_client = blob_service_client.get_blob_client(container=self.container_name, blob=blob_path)
        
        blob_client.upload_blob(json.dumps(data, indent=4), overwrite=True)
        logging.info("OpenStreetMap mapping asset ingestion complete.")

# This tells Python to execute the code ONLY when run directly via the terminal
if __name__ == "__main__":
    import sys
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    
    logging.basicConfig(level=logging.INFO)
    print("Testing OpenStreetMap Ingestor locally...")
    
    try:
        ingestor = OpenStreetMapIngestor()
        ingestor.execute_source_to_raw()
        print("Success! Check your Azure Storage Raw container.")
    except Exception as e:
        print(f"Test Failed: {str(e)}")