import os
import sys
import logging
import azure.functions as func

# Force Azure Function App to recognize the src folder as a valid module path for imports, allowing for a clean separation of custom ingestion logic within the src directory while still enabling seamless integration with the Azure Functions runtime environment. This setup ensures that the custom ingestor classes defined in the src folder can be imported and utilized within the function app without encountering module resolution issues, facilitating a modular and organized codebase for the data ingestion pipelines.
script_dir = os.path.dirname(os.path.abspath(__file__))
if script_dir not in sys.path:
    sys.path.insert(0, script_dir)
# -------------------------------------------------

# Import custom modules out of  src folder structure
from src.TrafikverketIngestor import TrafikverketIngestor
from src.OpenWeatherMapIngestor import OpenWeatherMapIngestor
from src.OpenStreetMapIngestor import OpenStreetMapIngestor

# Initialize the Azure Function App with HTTP trigger and set the authentication level to FUNCTION, which requires a function key for access, ensuring that only authorized requests can trigger the ingestion pipelines defined within the app.
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# Define an HTTP-triggered function named "RunIngestor" that serves as the entry point for executing the various data ingestion pipelines based on the input parameters provided in the request body. The function processes the incoming request, determines which ingestor to execute based on the 'ingestor_type' parameter, and handles any errors that may occur during execution, returning appropriate HTTP responses to indicate success or failure of the operation.
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

    # Route the execution to the appropriate ingestor class based on the 'ingestor_type' parameter, allowing for flexible execution of different data pipelines (Trafikverket, OpenWeatherMap, OpenStreetMap) through a single HTTP endpoint, while also providing error handling to manage unsupported ingestor types and any exceptions that may arise during processing.
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