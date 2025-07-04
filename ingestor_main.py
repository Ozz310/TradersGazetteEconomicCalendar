import os
import requests
import json
from flask import Flask, jsonify, request
from google.cloud import storage
from datetime import datetime

# --- Flask App Setup ---
app = Flask(__name__)

# --- Configuration ---
FRED_API_KEY = os.environ.get("FRED_API_KEY") # Your FRED API key
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME") # Your GCS bucket name

storage_client = storage.Client()

# --- Economic Indicator Definitions ---
FRED_SERIES = {
    "US_UNEMPLOYMENT_RATE": "UNRATE",                 # Unemployment Rate
    "US_CPI_ALL_ITEMS": "CPIAUCSL",                   # Consumer Price Index (All Urban Consumers)
    "US_GDP": "GDP",                                  # Gross Domestic Product
    "US_FED_FUNDS_RATE": "FEDFUNDS",                  # Federal Funds Rate
    "US_RETAIL_SALES": "RSXFS",                       # Retail Sales: Total (Excluding Food Services)
    "US_MANUFACTURING_PMI_ISM": "NAPM",               # ISM Manufacturing PMI (Composite) - FRED often has this
    "US_TRADE_BALANCE": "BOPASTB",                    # Balance of Trade in Goods and Services
    "US_HOUSING_STARTS": "HOUST",                     # Housing Starts: Total
    "US_CONSUMER_CONFIDENCE": "CONFCERT",            # Consumer Confidence Index
    "US_AVERAGE_HOURLY_EARNINGS_MOM": "CES0500000003", # Average Hourly Earnings of All Employees: Total Private (MoM)
    "US_NONFARM_PAYROLLS": "PAYEMS",                  # All Employees, Total Nonfarm (NFP)
    "US_INITIAL_JOBLESS_CLAIMS": "ICSA",              # Initial Jobless Claims
    "US_CONTINUING_JOBLESS_CLAIMS": "CCSA"            # Continuing Jobless Claims
}

ECB_SERIES = {
    "EU_HICP": {
        "flow_ref": "ICP",
        "key_values": "M.U2.N.000000.4.ANR" 
    },
    "EU_EUR_USD_FX_RATE": {
        "flow_ref": "EXR",
        "key_values": "D.USD.EUR.SP00.A"
    }
}

# --- Helper Functions (No Changes Here) ---

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
        "sort_order": "desc",
        "limit": 500
    }

    try:
        print(f"Fetching FRED data for series: {series_id}")
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json().get('observations', [])
        
        processed_data = []
        for obs in data:
            if obs['value'] != '.':
                processed_data.append({
                    "date": obs['date'],
                    "value": float(obs['value']),
                    "series_id": series_id,
                    "source": "FRED"
                })
        print(f"Successfully fetched {len(processed_data)} observations for {series_id}.")
        return processed_data
    except requests.exceptions.RequestException as e:
        print(f"ERROR fetching FRED data for {series_id}: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"ERROR decoding FRED JSON for {series_id}: {e}")
        print(f"FRED Response content: {response.text}")
        return []

def fetch_ecb_data(flow_ref, key_values):
    """Fetches data from ECB SDW API for a given flow and key values."""
    base_url = f"https://sdw-wsrest.ecb.europa.eu/service/data/{flow_ref}/{key_values}"
    headers = {"Accept": "application/json"}

    try:
        print(f"Fetching ECB data from URL: {base_url}")
        response = requests.get(base_url, headers=headers)
        response.raise_for_status()

        data = response.json()
        print(f"ECB API raw response status: {response.status_code}")
        # print(f"ECB API raw response content (first 500 chars): {response.text[:500]}") # Uncomment for verbose

        processed_data = []
        try:
            data_sets = data.get('dataSets', [])
            if not data_sets:
                print(f"ECB PARSING ERROR: No 'dataSets' found for {flow_ref}/{key_values}. Raw response might be empty or malformed.")
                return []
            
            series_data = data_sets[0].get('series', {})
            if not series_data:
                print(f"ECB PARSING ERROR: No 'series' data in first dataSet for {flow_ref}/{key_values}. Raw response might be empty or malformed.")
                return []

            for series_key, series_value in series_data.items():
                observations = series_value.get('observations', {})
                if not observations:
                    print(f"ECB PARSING WARNING: No 'observations' found in series {series_key} for {flow_ref}/{key_values}.")
                    continue

                for obs_key_index_str, obs_val_index_list in observations.items():
                    actual_value_index = obs_val_index_list[0]
                    value_obj = data_sets[0]['observations'].get(str(actual_value_index))
                    if not value_obj:
                        print(f"ECB PARSING WARNING: Missing value object for obs_key_index {obs_key_index_str} in dataSets[0].observations.")
                        continue

                    value = value_obj[0]
                    
                    time_period_ref_index = int(obs_key_index_str.split(":")[0])
                    if time_period_ref_index >= len(data['structure']['dimensions']['observation'][0]['values']):
                        print(f"ECB PARSING ERROR: Time period index {time_period_ref_index} out of bounds for structure.dimensions.observation values.")
                        continue

                    time_period_str = data['structure']['dimensions']['observation'][0]['values'][time_period_ref_index]['name']
                    
                    processed_data.append({
                        "date": time_period_str,
                        "value": float(value),
                        "flow_ref": flow_ref,
                        "key_values": key_values,
                        "source": "ECB"
                    })
            processed_data.sort(key=lambda x: x['date'])
            print(f"Successfully fetched and parsed {len(processed_data)} observations for ECB {flow_ref}/{key_values}.")
            return processed_data
        except (KeyError, IndexError, ValueError, TypeError) as parse_error:
            print(f"CRITICAL ECB PARSING ERROR for {flow_ref}/{key_values}: {parse_error}")
            # print(f"Problematic JSON structure snippet: {json.dumps(data, indent=2)[:1000]}...") # Uncomment for verbose
            return []

    except requests.exceptions.RequestException as e:
        print(f"ERROR fetching ECB data for {flow_ref}/{key_values} (Network/HTTP Error): {e}")
        # print(f"ECB API response text on error: {response.text}") # Uncomment for very verbose error
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

    if request.method == 'POST':
        print("Received manual POST request. Proceeding with ingestion...")
    else:
        return "Method Not Allowed", 405

    ingestion_results = {}

    # --- Fetch and Upload FRED Data ---
    for name, series_id in FRED_SERIES.items():
        print(f"Attempting to fetch FRED series: {name} ({series_id})")
        data = fetch_fred_data(series_id)
        if data:
            filename = f"economic_data/fred/{name.lower()}.json"
            if upload_to_gcs(data, filename):
                ingestion_results[name] = {"status": "success", "count": len(data), "gcs_path": filename}
            else:
                ingestion_results[name] = {"status": "failed_upload", "message": "GCS upload failed."}
        else:
            ingestion_results[name] = {"status": "failed_fetch", "message": f"No data fetched from FRED for {name} or API error."}

    # --- Fetch and Upload ECB Data ---
    for name, config in ECB_SERIES.items():
        print(f"Attempting to fetch ECB series: {name} (Flow: {config['flow_ref']}, Keys: {config['key_values']})")
        data = fetch_ecb_data(config["flow_ref"], config["key_values"])
        if data:
            filename = f"economic_data/ecb/{name.lower()}.json"
            if upload_to_gcs(data, filename):
                ingestion_results[name] = {"status": "success", "count": len(data), "gcs_path": filename}
            else:
                ingestion_results[name] = {"status": "failed_upload", "message": "GCS upload failed."}
        else:
            ingestion_results[name] = {"status": "failed_fetch", "message": f"No data fetched from ECB for {name} or API error."}

    print(f"Ingestion process finished at {datetime.now()} UTC")
    return jsonify({"ingestion_summary": ingestion_results}), 200

# --- Health Check Endpoint (for Cloud Run) ---
@app.route('/')
def health_check():
    return "Economic Data Ingestor is running and awaiting Pub/Sub triggers!"

# --- Entry point for local testing ---
if __name__ == '__main__':
    os.environ["FRED_API_KEY"] = "YOUR_FRED_API_KEY_HERE_FOR_LOCAL_TESTING"
    os.environ["GCS_BUCKET_NAME"] = "YOUR_GCS_BUCKET_NAME_HERE_FOR_LOCAL_TESTING"
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
