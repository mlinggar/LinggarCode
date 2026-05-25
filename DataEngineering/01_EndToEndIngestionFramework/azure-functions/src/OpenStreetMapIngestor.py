import os
import json
import logging
import requests
from datetime import datetime, timezone
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

class OpenStreetMapIngestor:
    def __init__(self):
        self.url = "https://overpass.openstreetmap.fr/api/interpreter"
        self.account_url = "https://sttrafficswedendev001.blob.core.windows.net/raw"
        self.container_name = "raw"

    def execute_source_to_raw(self):
        logging.info("Formulating Overpass QL query for national Sweden highway assets...")
        overpass_query = """
        [out:json][timeout:180];
        area["ISO3166-1"="SE"]->.searchArea;
        (
          node["highway"="speed_camera"](area.searchArea);
          node["highway"="toll_gantry"](area.searchArea);
        );
        out body;
        """
        headers = {
            "User-Agent": "NordicTrafficDataFramework/1.0 (linggar.pangestu@hyperisland.se)"
        }
        
        logging.info("Sending query to OpenStreetMap Overpass API for entire Sweden area. Waiting for response...")
        response = requests.post(self.url, data=overpass_query.encode('utf-8'), headers=headers)
        
        if response.status_code != 200:
            raise Exception(f"OpenStreetMap Overpass API returned an error: {response.status_code} - {response.text}")
            
        data = response.json()
        current_time = datetime.now(timezone.utc)
        data["extracted_at_utc"] = current_time.isoformat()
        
        blob_path = (
            f"osm/"
            f"year={current_time.year}/"
            f"month={current_time.month:02d}/"
            f"day={current_time.day:02d}/"
            f"sweden_osm_assets_{current_time.strftime('%H%M%S')}.json"
        )
        
        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(account_url=self.account_url, credential=credential)
        blob_client = blob_service_client.get_blob_client(container=self.container_name, blob=blob_path)
        blob_client.upload_blob(json.dumps(data, indent=4), overwrite=True)
        logging.info("OpenStreetMap mapping asset ingestion complete.")