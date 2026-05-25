import os
import sys
import logging
import azure.functions as func

# --- FORCE AZURE LINUX TO FIND YOUR SRC FOLDER ---
script_dir = os.path.dirname(os.path.abspath(__file__))
if script_dir not in sys.path:
    sys.path.insert(0, script_dir)
# -------------------------------------------------

# Import your custom modules out of your src folder structure
from src.TrafikverketIngestor import TrafikverketIngestor
from src.OpenWeatherMapIngestor import OpenWeatherMapIngestor
from src.OpenStreetMapIngestor import OpenStreetMapIngestor

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="RunIngestor", methods=["POST"])
def RunIngestor(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("RunIngestor HTTP trigger function processing a request.")

    try:
        req_body = req.get_json()
        ingestor_type = req_body.get('ingestor_type')
    except ValueError:
        return func.HttpResponse("Execution Rejected: Missing or malformed JSON payload.", status_code=400)

    if not ingestor_type:
        return func.HttpResponse("Execution Rejected: 'ingestor_type' parameter is missing.", status_code=400)

    # Clean up the input string to match what ADF sends without case-sensitivity breaking it
    target_ingestor = ingestor_type.strip().lower()

    try:
        if target_ingestor == 'trafikverket':
            logging.info("Routing execution pipeline to TrafikverketIngestor...")
            ingestor = TrafikverketIngestor()
            ingestor.execute_source_to_raw()
            
        elif target_ingestor in ['openweathermap', 'weather']:
            logging.info("Routing execution pipeline to OpenWeatherMapIngestor...")
            ingestor = OpenWeatherMapIngestor()
            ingestor.execute_source_to_raw()
            
        elif target_ingestor in ['openstreetmap', 'osm']:
            logging.info("Routing execution pipeline to OpenStreetMapIngestor...")
            ingestor = OpenStreetMapIngestor()
            ingestor.execute_source_to_raw()
            
        else:
            return func.HttpResponse(f"Execution Rejected: Target engine '{ingestor_type}' is unsupported.", status_code=400)

        return func.HttpResponse(body=f"Success: Cloud ingestion engine for '{ingestor_type}' completed operations.", status_code=200)

    except Exception as e:
        logging.error(f"Critical operational error within custom modules: {str(e)}")
        return func.HttpResponse(body=f"Pipeline Processing Error: {str(e)}", status_code=500)