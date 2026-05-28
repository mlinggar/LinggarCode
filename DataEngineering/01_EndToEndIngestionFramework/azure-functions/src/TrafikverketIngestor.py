import os
import json
import logging
from datetime import datetime
import requests
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

class TrafikverketIngestor:
    def __init__(self):
        self.api_key = "b6c5ec01133945088f6f5c171325e535"
        self.url = "https://api.trafikinfo.trafikverket.se/v2/data.json"
        self.account_url = "https://sttrafficswedendev001.blob.core.windows.net/raw"
        self.container_name = "raw"

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
        
        headers = {'Content-Type': 'text/xml'}
        logging.info("Dispatching Stockholm-scoped payload to Trafikverket API...")
        response = requests.post(self.url, data=xml_query, headers=headers)
        
        if response.status_code != 200:
            raise Exception(f"Trafikverket API error: {response.status_code} - {response.text}")
            
        data = response.json()
        
        if "RESPONSE" in data and "RESULT" in data["RESPONSE"]:
            for block in data["RESPONSE"]["RESULT"]:
                if "ERROR" in block:
                    raise Exception(f"Trafikverket Server Error: {block['ERROR']['MESSAGE']}")

        data["extracted_at_utc"] = datetime.utcnow().isoformat()
        data["data_sources_combined"] = ["TravelTimeRoute", "RoadCondition"]
        data["targeted_region"] = "Stockholm County (Län 1)"
        
        current_time = datetime.utcnow()
        blob_path = (
            f"road/traffic_weather_impact/"
            f"year={current_time.year}/"
            f"month={current_time.month:02d}/"
            f"day={current_time.day:02d}/"
            f"stockholm_traffic_impact_{current_time.strftime('%H%M%S')}.json"
        )
        
        logging.info(f"Authenticating to Azure Lakehouse. Staging path: {blob_path}")
        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(account_url=self.account_url, credential=credential)
        blob_client = blob_service_client.get_blob_client(container=self.container_name, blob=blob_path)
        
        logging.info("Streaming Stockholm traffic JSON matrix to raw landing zone...")
        blob_client.upload_blob(json.dumps(data, indent=4), overwrite=True)
        logging.info("Ingestion cycle complete. Stockholm data successfully landed.")

if __name__ == "__main__":
    try:
        pipeline = TrafikverketIngestor()
        pipeline.execute_source_to_raw()
    except Exception as err:
        logging.critical(f"Pipeline crashed during execution: {err}")