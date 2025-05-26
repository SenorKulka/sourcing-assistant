import os
import json
import tempfile
import re
import logging
from dotenv import load_dotenv
from flask import Flask, request, jsonify, send_from_directory
from src.lovbuy_client import LovbuyClient
from src.update_google_sheet import run_sheet_update
from src.logging_config import setup_logging

# Set up logging
logger = setup_logging()

load_dotenv(override=True)

app = Flask(__name__, static_folder='frontend')

LOVBUY_API_KEY = os.getenv("LOVBUY_API_KEY")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

def extract_sheet_id_from_url(url_string):
    if not url_string:
        return None
    
    match = re.search(r'/spreadsheets/d/([a-zA-Z0-9-_]+)', url_string)
    if match:
        return match.group(1)
    
    match = re.search(r'/d/([a-zA-Z0-9-_]+)', url_string)
    if match:
        return match.group(1)
        
    if re.fullmatch(r'[a-zA-Z0-9-_]{44}', url_string):
         return url_string
         
    logger.warning(f"Could not parse a valid Google Sheet ID from '{url_string}'. It might be an invalid URL or ID format.")
    return None

def process_sourcing_request(url, product_name, min_moq, max_moq, google_sheet_id):
    logger.info(f"Processing URL: {url} for product: {product_name}, MOQ: {min_moq}-{max_moq}, Sheet ID: {google_sheet_id}")

    # Default min_moq to 120 if not provided
    if min_moq is None:
        min_moq = 120
        logger.info(f"min_moq was not provided, defaulting to {min_moq}")

    if not LOVBUY_API_KEY:
        error_msg = "Server configuration error: LOVBUY_API_KEY not set."
        print(f"Error: {error_msg}")
        return {"error": error_msg}, 500

    if not google_sheet_id: 
        error_msg = "Client error: Google Sheet ID is missing or invalid."
        print(f"Error: {error_msg}")
        return {"error": error_msg}, 400

    if not GOOGLE_APPLICATION_CREDENTIALS:
        error_msg = "Server configuration error: GOOGLE_APPLICATION_CREDENTIALS not set."
        print(f"Error: {error_msg}")
        return {"error": error_msg}, 500

    lovbuy = LovbuyClient(LOVBUY_API_KEY)
    response_data = {}
    status_code = 200

    try:
        product_info = lovbuy.get_product_info_from_1688_url(url)

        if product_info:
            print(f"Successfully fetched product data from LovBuy for URL: {url}")
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json', encoding='utf-8') as tmp_file:
                json.dump(product_info, tmp_file, ensure_ascii=False, indent=2)
                temp_product_data_path = tmp_file.name
            
            print(f"Product data temporarily saved to: {temp_product_data_path}")
            print(f"Attempting to update Google Sheet... Product: {product_name}, MinMOQ: {min_moq}, MaxMOQ: {max_moq}")

            try:
                stats = run_sheet_update(
                    product_data_path=temp_product_data_path,
                    product_type_arg=product_name,
                    min_moq_arg=min_moq,
                    max_moq_arg=max_moq,
                    source_url_arg=url,
                    google_sheet_id_param=google_sheet_id
                )
                
                # Create detailed success message with statistics
                if stats and not stats.get('error'):
                    message = f"✅ Successfully processed '{stats['product_name']}' - {stats['skus_found']} SKUs found → {stats['skus_after_filter']} after filtering → {stats['rows_uploaded']} uploaded to Google Sheet"
                else:
                    message = f"⚠️ Processing completed with issues for '{stats.get('product_name', 'Unknown Product')}': {stats.get('error', 'Unknown error')}"
                
                response_data = {"message": message}
                print(message)
            except Exception as e_sheet:
                error_message = f"An error occurred during Google Sheet update for {url}: {e_sheet}"
                print(error_message)
                response_data = {"error": error_message}
                status_code = 500
            finally:
                if os.path.exists(temp_product_data_path):
                    os.remove(temp_product_data_path)
                    print(f"Temporary file {temp_product_data_path} removed.")

        else:
            error_message = f"Failed to retrieve product information from LovBuy for URL: {url}"
            print(error_message)
            response_data = {"error": error_message}
            status_code = 400

    except Exception as e_lovbuy:
        error_message = f"An error occurred while fetching product data from LovBuy for {url}: {e_lovbuy}"
        print(error_message)
        response_data = {"error": error_message}
        status_code = 500
    
    return response_data, status_code

@app.route('/')
def serve_index():
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/api/process', methods=['POST'])
def handle_api_process():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid JSON payload"}), 400

    urls = data.get('url', '').split()  # Split by any whitespace
    product_name = data.get('productName')
    min_moq_str = data.get('minMoq')
    max_moq_str = data.get('maxMoq')
    gsheet_link_or_id = data.get('gsheetLink')

    if not urls:
        return jsonify({"error": "No URLs provided in the request"}), 400
    if not product_name: 
        logger.info("Starting processing...")

    parsed_google_sheet_id = extract_sheet_id_from_url(gsheet_link_or_id)
    if not parsed_google_sheet_id:
        return jsonify({"error": f"Invalid or missing Google Sheet link/ID: '{gsheet_link_or_id}'"}), 400

    try:
        min_moq = int(min_moq_str) if min_moq_str else None
        max_moq = int(max_moq_str) if max_moq_str else None
    except ValueError:
        return jsonify({"error": "Invalid 'minMoq' or 'maxMoq' value. Must be an integer."}), 400

    # Process all URLs and collect responses
    responses = []
    for url in urls:
        url = url.strip()
        if not url:
            continue
            
        response, status = process_sourcing_request(url, product_name, min_moq, max_moq, parsed_google_sheet_id)
        responses.append({
            "url": url,
            "status": "success" if status == 200 else "error",
            "message": response.get("message") or response.get("error"),
            "status_code": status
        })
    
    # Check if all requests were successful
    all_success = all(r["status"] == "success" for r in responses)
    status_code = 200 if all_success else 207  # 207 Multi-Status if some failed
    
    return jsonify({
        "results": responses,
        "message": f"Processed {len(responses)} URLs" + ("" if all_success else " (some failed)")
    }), status_code

if __name__ == "__main__":
    app.run(debug=True, port=5000)
