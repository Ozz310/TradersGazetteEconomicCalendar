import os
import json
from flask import Flask, jsonify, request
from flask_cors import CORS # Import Flask-Cors for handling CORS
from google.cloud import storage
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app) # Enable CORS for all routes

# --- Configuration ---
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")

# --- GCS Client ---
storage_client = storage.Client()

# --- In-memory cache for economic data ---
# This cache will store data to serve quickly without re-reading from GCS every time.
# Data will expire after CACHE_TTL_SECONDS to ensure freshness.
data_cache = {}
CACHE_TTL_SECONDS = 3600 # Cache data for 1 hour (3600 seconds)

# --- Define the FRED Series IDs and their human-readable names ---
# These should match the files uploaded by your ingestor_main.py
FRED_SERIES_MAP = {
    "us_unemployment_rate": {"series_id": "UNRATE", "name": "Unemployment Rate", "unit": "%", "frequency": "Monthly"},
    "us_cpi_all_items": {"series_id": "CPIAUCSL", "name": "CPI (All Items)", "unit": "Index", "frequency": "Monthly"},
    "us_gdp": {"series_id": "GDP", "name": "Gross Domestic Product", "unit": "Billions of Dollars", "frequency": "Quarterly"},
    "us_fed_funds_rate": {"series_id": "FEDFUNDS", "name": "Fed Funds Rate", "unit": "%", "frequency": "Daily"},
    "us_retail_sales": {"series_id": "RSXFS", "name": "Retail Sales Ex. Food Services", "unit": "Billions of Dollars", "frequency": "Monthly"},
    "us_manufacturing_pmi_ism": {"series_id": "NAPM", "name": "ISM Manufacturing PMI", "unit": "Index", "frequency": "Monthly"},
    "us_trade_balance": {"series_id": "BOPASTB", "name": "Trade Balance", "unit": "Millions of Dollars", "frequency": "Monthly"},
    "us_housing_starts": {"series_id": "HOUST", "name": "Housing Starts", "unit": "Thousands of Units", "frequency": "Monthly"},
    "us_consumer_confidence": {"series_id": "CONFCERT", "name": "Consumer Confidence", "unit": "Index", "frequency": "Monthly"},
    "us_average_hourly_earnings_mom": {"series_id": "CES0500000003", "name": "Avg. Hourly Earnings (MoM)", "unit": "%", "frequency": "Monthly"},
    "us_nonfarm_payrolls": {"series_id": "PAYEMS", "name": "Nonfarm Payrolls", "unit": "Thousands", "frequency": "Monthly"},
    "us_initial_jobless_claims": {"series_id": "ICSA", "name": "Initial Jobless Claims", "unit": "Thousands", "frequency": "Weekly"},
    "us_continuing_jobless_claims": {"series_id": "CCSA", "name": "Continuing Jobless Claims", "unit": "Thousands", "frequency": "Weekly"}
}

# --- Helper Functions ---

def load_data_from_gcs(filename):
    """Loads a JSON object from Google Cloud Storage."""
    if not GCS_BUCKET_NAME:
        print("ERROR: GCS_BUCKET_NAME not set. Cannot load data from GCS.")
        return None
    
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(filename)
    
    try:
        if not blob.exists():
            print(f"WARNING: File {filename} not found in GCS bucket {GCS_BUCKET_NAME}.")
            return None
        
        data = blob.download_as_text()
        return json.loads(data)
    except Exception as e:
        print(f"ERROR loading {filename} from GCS: {e}")
        return None

def get_cached_data(key):
    """Retrieves data from cache if not expired."""
    if key in data_cache:
        cached_item = data_cache[key]
        if datetime.now() < cached_item['expiry']:
            return cached_item['data']
    return None

def set_cached_data(key, data):
    """Stores data in cache with an expiry time."""
    data_cache[key] = {
        'data': data,
        'expiry': datetime.now() + timedelta(seconds=CACHE_TTL_SECONDS)
    }

# --- API Endpoints ---

@app.route('/api/economic-calendar/us', methods=['GET'])
def get_us_economic_data():
    """
    Endpoint to retrieve US economic data.
    Can be filtered by indicator (e.g., ?indicator=US_CPI_ALL_ITEMS).
    """
    requested_indicator = request.args.get('indicator')
    
    # Try to serve from cache first
    cache_key = f"us_data_{requested_indicator or 'all'}"
    cached_response = get_cached_data(cache_key)
    if cached_response:
        print(f"Serving US data from cache for {cache_key}")
        return jsonify(cached_response)

    all_us_data = []
    
    for key, info in FRED_SERIES_MAP.items():
        # If a specific indicator is requested, only fetch that one
        if requested_indicator and requested_indicator != key:
            continue

        filename = f"economic_data/fred/{key.lower()}.json"
        data = load_data_from_gcs(filename)
        
        if data:
            # Add metadata to each data point for easier frontend processing
            for item in data:
                item['indicator_name'] = info['name']
                item['unit'] = info['unit']
                item['frequency'] = info['frequency']
                item['country'] = "US"
                item['currency'] = "USD" # Assuming USD for US data
            all_us_data.extend(data)
        else:
            print(f"Could not load data for {key} from GCS.")

    # Sort data by date (most recent first)
    all_us_data.sort(key=lambda x: x['date'], reverse=True)

    # Cache the response before returning
    response_data = {"status": "success", "data": all_us_data}
    set_cached_data(cache_key, response_data)
    print(f"Successfully loaded and cached US data for {cache_key}")
    return jsonify(response_data)

@app.route('/')
def health_check():
    return "Economic Calendar API is running!"

# --- Entry point for local testing ---
if __name__ == '__main__':
    # Set dummy environment variables for local testing
    os.environ["GCS_BUCKET_NAME"] = "YOUR_GCS_BUCKET_NAME_HERE_FOR_LOCAL_TESTING" # Replace with your actual bucket for local run
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))