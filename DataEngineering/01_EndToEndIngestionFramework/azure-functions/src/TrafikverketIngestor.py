import os
import json
import logging
from datetime import datetime
import requests
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

# --- This module is responsible for ingesting traffic and road condition data from Trafikverket's API, specifically targeting the Stockholm County region to align with the geospatial and weather data ingested from OpenStreetMap and OpenWeatherMap. ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# The TrafikverketIngestor class encapsulates the logic for fetching traffic and road condition data for Stockholm County, enriching it with metadata, and uploading it to Azure Blob Storage in a structured format for downstream processing and analysis. It combines multiple data sources from the API into a single cohesive dataset to provide a comprehensive view of traffic conditions in the targeted region.
class TrafikverketIngestor:
    # The constructor initializes the API key for Trafikverket, defines the API endpoint, and sets up the Azure Blob Storage connection details, ensuring that the ingestor is configured to operate within the defined scope of the project.
    def __init__(self):
        self.api_key = "b6c5ec01133945088f6f5c171325e535"
        self.url = "https://api.trafikinfo.trafikverket.se/v2/data.json"
        self.account_url = "https://sttrafficswedendev001.blob.core.windows.net/raw"
        self.container_name = "raw"

    # The execute_source_to_raw method formulates a specific XML payload to retrieve both TravelTimeRoute and RoadCondition data for Stockholm County from the Trafikverket API, processes the response, adds metadata, and uploads the resulting JSON data to Azure Blob Storage in a structured path format that organizes the data by date for efficient retrieval and management. It also includes error handling to log any issues encountered during the API call or data processing steps.
    def execute_source_to_raw(self):
        logging.info("Formulating dual-query XML payload for Stockholm Region Traffic and Road Conditions...")
        
        # Isolated completely to Stockholm County (CountyNo = 1)
        xml_query = f"""
        <REQUEST>
            <LOGIN authenticationkey="{self.api_key}" />
            
            <QUERY objecttype="TravelTimeRoute" schemaversion="1.5">
                <FILTER>
                    <AND>
                        <EQ name="Deleted" value="false" />
                        <EQ name="CountyNo" value="1" />
                    </AND>
                </FILTER>
            </QUERY>
            
            <QUERY objecttype="RoadCondition" schemaversion="1.2">
                <FILTER>
                    <AND>
                        <EQ name="Deleted" value="false" />
                        <EQ name="CountyNo" value="1" />
                    </AND>
                </FILTER>
            </QUERY>
        </REQUEST>
        """
        # Set a custom User-Agent header to identify the application making the request, which can help with troubleshooting and ensuring compliance with API usage policies.
        headers = {'Content-Type': 'text/xml'}
        logging.info("Dispatching Stockholm-scoped payload to Trafikverket API...")
        response = requests.post(self.url, data=xml_query, headers=headers)
        
        # Check if the API response is successful; if not, raise an exception with details for troubleshooting. Additionally, check for any server-side errors reported within the response body to ensure that the data being processed is valid and complete.
        if response.status_code != 200:
            raise Exception(f"Trafikverket API error: {response.status_code} - {response.text}")
            
        data = response.json()
        
        # Perform a check for any server-reported errors within the response body, which may indicate issues with the query or data retrieval that are not reflected in the HTTP status code, ensuring that only valid and complete data is processed and ingested into Azure Blob Storage.
        if "RESPONSE" in data and "RESULT" in data["RESPONSE"]:
            for block in data["RESPONSE"]["RESULT"]:
                if "ERROR" in block:
                    raise Exception(f"Trafikverket Server Error: {block['ERROR']['MESSAGE']}")

        # Enrich the response with metadata
        data["extracted_at_utc"] = datetime.utcnow().isoformat()
        data["data_sources_combined"] = ["TravelTimeRoute", "RoadCondition"]
        data["targeted_region"] = "Stockholm County (Län 1)"
        
        # Construct a blob path that organizes the ingested traffic data by date, following a consistent naming convention for easy retrieval and management in Azure Blob Storage. The path includes a specific folder for road traffic impact data to align with the overall project structure and facilitate downstream processing and analysis.
        current_time = datetime.utcnow()
        blob_path = (
            f"road/traffic_weather_impact/"
            f"year={current_time.year}/"
            f"month={current_time.month:02d}/"
            f"day={current_time.day:02d}/"
            f"stockholm_traffic_impact_{current_time.strftime('%H%M%S')}.json"
        )
        
        # Log the connection attempt to Azure Blob Storage and upload the enriched data as a JSON file, ensuring that any existing file with the same name is overwritten to maintain the latest dataset for the targeted region.
        logging.info(f"Authenticating to Azure Lakehouse. Staging path: {blob_path}")
        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(account_url=self.account_url, credential=credential)
        blob_client = blob_service_client.get_blob_client(container=self.container_name, blob=blob_path)
        
        # Log the connection attempt to Azure Blob Storage and upload the enriched data as a JSON file, ensuring that any existing file with the same name is overwritten to maintain the latest dataset for the targeted region.
        logging.info("Streaming Stockholm traffic JSON matrix to raw landing zone...")
        blob_client.upload_blob(json.dumps(data, indent=4), overwrite=True)
        logging.info("Ingestion cycle complete. Stockholm data successfully landed.")

# This block allows the module to be run independently for testing purposes, executing the ingestion process directly when the script is executed as the main program.
if __name__ == "__main__":
    try:
        pipeline = TrafikverketIngestor()
        pipeline.execute_source_to_raw()
    except Exception as err:
        logging.critical(f"Pipeline crashed during execution: {err}")