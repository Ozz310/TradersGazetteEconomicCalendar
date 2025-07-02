import os
import requests
import json
from flask import Flask, jsonify, request
from google.cloud import storage
from datetime import datetime

# --- Flask App Setup ---
app = Flask(__name__)

# --- Configuration ---
# Your FRED API key (required for FRED data)
# This will be set as an environment variable in Cloud Run
FRED_API_KEY = os.environ.get("FRED_API_KEY")
# Your GCS bucket name for storing data
# This will also be set as an environment variable in Cloud Run
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")

# --- GCS Client ---
# Initialize the GCS client. Cloud Run handles authentication automatically.
storage_client = storage.Client()

# --- Economic Indicator Definitions ---
# FRED Series IDs for US data
FRED_SERIES = {
    "US_UNEMPLOYMENT_RATE": "UNRATE", # Unemployment Rate
    "US_CPI": "CPIAUCSL"            # Consumer Price Index (All Urban Consumers)
}

# ECB SDW Dataflow and KeyValue pairs for Euro Area data
# Finding the exact codes can be tricky. These are examples.
# You might need to explore data.ecb.europa.eu to confirm specific series.
ECB_SERIES = {
    # Harmonised Index of Consumer Prices (HICP) - Euro Area, All-items, Annual rate of change
    "EU_HICP": {
        "flow_ref": "ICP",
        "key_values": "M.U2.N.000000.4.ANR" # M=Monthly, U2=Euro Area, N=National, 000000=all items, 4=HICP, ANR=Annual rate of change
    },
    # Example: Daily FX Ref Rate, Euro Area (U2), US Dollar (USD), Reference rate (EUR_USD_N_A)
    "EU_EUR_USD_FX_RATE": {
        "flow_ref": "EXR",
        "key_values": "D.USD.EUR.SP00.A" # D=Daily, USD=Currency, EUR=Euro, SP00=Spot, A=Average
    }
}


# --- Helper Functions ---

def upload_to_gcs(data, filename):
    """Uploads a JSON object to Google Cloud Storage."""
    if not GCS_BUCKET_NAME:
        print("ERROR: GCS_BUCKET_NAME environment variable not set. Cannot upload data.")
        return False
    
    try:
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(filename)
        
        blob.upload_from_string(json.dumps(data), content_type="application/json")
        print(f"Successfully uploaded {filename} to GCS bucket {GCS_BUCKET_NAME}")
        return True
    except Exception as e:
        print(f"ERROR uploading {filename} to GCS: {e}")
        # Add more specific error logging if possible (e.g., permissions)
        return False

def fetch_fred_data(series_id):
    """Fetches data for a given FRED series ID."""
    if not FRED_API_KEY:
        print(f"WARNING: FRED_API_KEY environment variable not set for series {series_id}. Skipping FRED fetch.")
        return []

    base_url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "desc", # Get newest first
        "limit": 500 # Fetch up to 500 observations
    }

    try:
        print(f"Fetching FRED data for series: {series_id}")
        response = requests.get(base_url, params=params)
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        data = response.json().get('observations', [])
        
        # Extract relevant fields and reverse order to be chronological if needed later for charts
        processed_data = []
        for obs in data:
            if obs['value'] != '.': # Filter out missing values, which FRED uses for N/A
                processed_data.append({
                    "date": obs['date'],
                    "value": float(obs['value']),
                    "series_id": series_id,
                    "source": "FRED"
                })
        print(f"Successfully fetched {len(processed_data)} observations for {series_id}.")
        return processed_data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching FRED data for {series_id}: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"Error decoding FRED JSON for {series_id}: {e}")
        print(f"FRED Response content: {response.text}") # Print full response for debugging
        return []

def fetch_ecb_data(flow_ref, key_values):
    """Fetches data from ECB SDW API for a given flow and key values."""
    base_url = f"https://sdw-wsrest.ecb.europa.eu/service/data/{flow_ref}/{key_values}"
    headers = {"Accept": "application/json"} # Request JSON format

    try:
        print(f"Fetching ECB data for flow: {flow_ref}, keys: {key_values}")
        response = requests.get(base_url, headers=headers)
        response.raise_for_status()

        data = response.json()
        
        processed_data = []
        # ECB data structure is complex (SDMX-JSON), requires careful parsing
        # This is a simplified parser for common time series structure for dataSets[0]
        try:
            data_sets = data.get('dataSets', [])
            if not data_sets:
                print(f"No dataSets found for ECB {flow_ref}/{key_values}.")
                return []
            
            series_data = data_sets[0].get('series', {})
            
            # Extract relevant dimensions and observation values
            for series_key, series_value in series_data.items():
                observations = series_value.get('observations', {})
                for obs_key_index_str, obs_val_index_list in observations.items():
                    # obs_key_index_str is a string like "0:0:0:0:0"
                    # obs_val_index_list is a list like [0] pointing to dataSets[0].observations
                    
                    # Extract the actual value from dataSets[0].observations
                    actual_value_index = obs_val_index_list[0] # Usually 0
                    value_obj = data_sets[0]['observations'].get(str(actual_value_index))
                    if value_obj:
                        value = value_obj[0] # The actual value is usually the first element in the list

                        # Map observation keys to time periods using 'structure'
                        # This part of ECB parsing can be tricky and requires understanding the structure.
                        # For now, we'll try to get the time dimension value.
                        time_period_ref_index = int(obs_key_index_str.split(":")[0]) # This index refers to the time period value in structure.dimensions.observation[0].values
                        time_period_str = data['structure']['dimensions']['observation'][0]['values'][time_period_ref_index]['name']
                        
                        processed_data.append({
                            "date": time_period_str, # e.g., "2023-M12", might need further parsing for full date objects
                            "value": float(value),
                            "flow_ref": flow_ref,
                            "key_values": key_values,
                            "source": "ECB"
                        })
            # Sort by date, assuming 'date' is comparable (e.g., "YYYY-MM" or "YYYY-MM-DD")
            processed_data.sort(key=lambda x: x['date'])
            print(f"Successfully fetched {len(processed_data)} observations for ECB {flow_ref}/{key_values}.")
            return processed_data
        except (KeyError, IndexError, ValueError, TypeError) as parse_error:
            print(f"ERROR: Could not parse ECB data for {flow_ref}/{key_values}. Data structure might have changed or is unexpected: {parse_error}")
            # print(json.dumps(data, indent=2)) # Uncomment this line to dump the full JSON for manual inspection if parsing fails
            return []

    except requests.exceptions.RequestException as e:
        print(f"Error fetching ECB data for {flow_ref}/{key_values}: {e}")
        return []

# --- Main Ingestion Logic (Cloud Run Entrypoint) ---

@app.route('/ingest-economic-data', methods=['POST'])
def ingest_economic_data():
    """
    Fetches economic data from FRED and ECB APIs and stores it in GCS.
    This endpoint is designed to be triggered by Cloud Scheduler via Pub/Sub,
    but this version is modified for direct manual POST testing.
    """
    print(f"Ingestion process started at {datetime.now()} UTC")

    # --- TEMPORARY MODIFICATION FOR MANUAL TESTING ---
    # Comment out or remove these lines when setting up with Cloud Scheduler/PubSub.
    if request.method == 'POST':
        print("Received manual POST request. Proceeding with ingestion...")
    else:
        return "Method Not Allowed", 405 # Ensure it's still POST only
    # --- END TEMPORARY MODIFICATION ---

    ingestion_results = {}

    # --- Fetch and Upload FRED Data ---
    for name, series_id in FRED_SERIES.items():
        data = fetch_fred_data(series_id)
        if data:
            # Filename example: economic_data/fred/us_unemployment_rate.json
            filename = f"economic_data/fred/{name.lower()}.json"
            if upload_to_gcs(data, filename):
                ingestion_results[name] = {"status": "success", "count": len(data), "gcs_path": filename}
            else:
                ingestion_results[name] = {"status": "failed_upload", "message": "GCS upload failed."}
        else:
            ingestion_results[name] = {"status": "failed_fetch", "message": "No data fetched from FRED or API error."}

    # --- Fetch and Upload ECB Data ---
    for name, config in ECB_SERIES.items():
        data = fetch_ecb_data(config["flow_ref"], config["key_values"])
        if data:
            # Filename example: economic_data/ecb/eu_hicp.json
            filename = f"economic_data/ecb/{name.lower()}.json"
            if upload_to_gcs(data, filename):
                ingestion_results[name] = {"status": "success", "count": len(data), "gcs_path": filename}
            else:
                ingestion_results[name] = {"status": "failed_upload", "message": "GCS upload failed."}
        else:
            ingestion_results[name] = {"status": "failed_fetch", "message": "No data fetched from ECB or API error."}

    print(f"Ingestion process finished at {datetime.now()} UTC")
    return jsonify({"ingestion_summary": ingestion_results}), 200

# --- Health Check Endpoint (for Cloud Run) ---
@app.route('/')
def health_check():
    return "Economic Data Ingestor is running and awaiting Pub/Sub triggers!"

# --- Entry point for local testing ---
if __name__ == '__main__':
    # When running locally, set dummy environment variables for testing.
    # In Cloud Run, these will be provided by the environment.
    os.environ["FRED_API_KEY"] = "YOUR_FRED_API_KEY_HERE_FOR_LOCAL_TESTING" # Replace with your actual key for local run
    os.environ["GCS_BUCKET_NAME"] = "YOUR_GCS_BUCKET_NAME_HERE_FOR_LOCAL_TESTING" # Replace with your actual bucket for local run
    
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
