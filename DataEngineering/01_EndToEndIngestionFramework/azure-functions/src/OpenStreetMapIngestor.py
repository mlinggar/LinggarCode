import os
import json
import logging
import requests
from datetime import datetime, timezone
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# --- This module is responsible for ingesting geospatial asset data from OpenStreetMap's Overpass API, specifically targeting the Stockholm County region. ---
class OpenStreetMapIngestor:
    # The constructor initializes the API endpoint and Azure Blob Storage connection details.
    def __init__(self):
        self.url = "https://overpass.openstreetmap.fr/api/interpreter"
        self.account_url = "https://sttrafficswedendev001.blob.core.windows.net/raw"
        self.container_name = "raw"

    # The execute_source_to_raw method formulates a specific Overpass QL query to retrieve nodes tagged as speed cameras and toll gantries within the administrative boundaries of Stockholm County. It then processes the response, adds metadata, and uploads the resulting JSON data to Azure Blob Storage in a structured path format.
    def execute_source_to_raw(self):
        logging.info("Formulating Overpass QL query targeted strictly at Stockholm County (Stockholms län)...")
        
        # Scoped to admin_level 4 (Counties of Sweden) named Stockholms län
        overpass_query = """
        [out:json][timeout:180];
        area["name"="Stockholms län"]["admin_level"="4"]->.searchArea;
        (
          node["highway"="speed_camera"](area.searchArea);
          node["highway"="toll_gantry"](area.searchArea);
        );
        out body;
        """
        headers = {
            "User-Agent": "NordicTrafficDataFramework/1.0 (linggar.pangestu@hyperisland.se)"
        }
        
        logging.info("Sending query to OpenStreetMap Overpass API for Stockholm region. Waiting for response...")
        response = requests.post(self.url, data=overpass_query.encode('utf-8'), headers=headers)
        
        # Check if the API response is successful; if not, raise an exception with details for troubleshooting.
        if response.status_code != 200:
            raise Exception(f"OpenStreetMap Overpass API returned an error: {response.status_code} - {response.text}")

        # Process the successful response, enrich it with metadata, and prepare it for upload to Azure Blob Storage.   
        data = response.json()
        current_time = datetime.now(timezone.utc)
        data["extracted_at_utc"] = current_time.isoformat()
        data["targeted_region"] = "Stockholms län"
        
        # Construct a blob path that organizes the ingested data by date and time, following a consistent naming convention for easy retrieval and management in Azure Blob Storage.
        blob_path = (
            f"osm/"
            f"year={current_time.year}/"
            f"month={current_time.month:02d}/"
            f"day={current_time.day:02d}/"
            f"stockholm_osm_assets_{current_time.strftime('%H%M%S')}.json"
        )
        
        # Log the connection attempt to Azure Blob Storage and upload the enriched data as a JSON file, ensuring that any existing file with the same name is overwritten to maintain the latest dataset.
        logging.info(f"Connecting to Data Lake to upload filtered regional assets: {blob_path}")
        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(account_url=self.account_url, credential=credential)
        blob_client = blob_service_client.get_blob_client(container=self.container_name, blob=blob_path)
        blob_client.upload_blob(json.dumps(data, indent=4), overwrite=True)
        logging.info("OpenStreetMap mapping asset ingestion complete for Stockholm.")

# This block allows the module to be run independently for testing purposes, executing the ingestion process directly when the script is executed as the main program.
if __name__ == "__main__":
    pipeline = OpenStreetMapIngestor()
    pipeline.execute_source_to_raw()