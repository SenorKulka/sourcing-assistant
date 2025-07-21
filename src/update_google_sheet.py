import os
import json
import datetime
import logging
from urllib.parse import urlparse, urlunparse
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Configure logging
logger = logging.getLogger(__name__)

# --- Configuration ---
load_dotenv(override=True)

# Environment variables
# GOOGLE_SHEET_ID_FROM_ENV = os.getenv("GOOGLE_SHEET_ID") # No longer primary source for run_sheet_update
GOOGLE_CREDENTIALS_PATH_FROM_ENV = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# logger.debug(f"GOOGLE_SHEET_ID from .env (raw): {{GOOGLE_SHEET_ID_FROM_ENV}}") # Commented out
logger.debug(f"GOOGLE_APPLICATION_CREDENTIALS from .env (raw): {GOOGLE_CREDENTIALS_PATH_FROM_ENV}")

# Global GOOGLE_SHEET_ID processing is removed as run_sheet_update will take it as a parameter.
# GOOGLE_SHEET_ID = None # Ensuring it's not used globally by mistake.

# Process GOOGLE_CREDENTIALS_PATH
# If the path from .env starts with './', make it absolute relative to the script's directory or CWD.
# For simplicity, we'll assume it's relative to the CWD where the script is run.
if GOOGLE_CREDENTIALS_PATH_FROM_ENV and GOOGLE_CREDENTIALS_PATH_FROM_ENV.startswith('./'):
    # Construct absolute path from CWD. Note: os.path.abspath handles './' correctly.
    GOOGLE_CREDENTIALS_PATH = os.path.abspath(GOOGLE_CREDENTIALS_PATH_FROM_ENV)
    logger.debug(f"Resolved GOOGLE_APPLICATION_CREDENTIALS (relative to CWD): {GOOGLE_CREDENTIALS_PATH}")
elif GOOGLE_CREDENTIALS_PATH_FROM_ENV:
    GOOGLE_CREDENTIALS_PATH = GOOGLE_CREDENTIALS_PATH_FROM_ENV
    logger.debug(f"Using GOOGLE_APPLICATION_CREDENTIALS as is (absolute or from global env): {GOOGLE_CREDENTIALS_PATH}")
else:
    GOOGLE_CREDENTIALS_PATH = None
    logger.warning("GOOGLE_APPLICATION_CREDENTIALS not found in .env or environment.")

# Script configurations
TARGET_SHEET_NAME = "Sheet1"  # The name of the sheet to update
EXPECTED_HEADER = ["id", "photo", "price 1688", "price cust", "profit", "moq", "info", "material", "link"]
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# --- Utility Functions ---

def try_convert_to_float(value):
    """
    Safely converts a value to float.
    Returns the float value if conversion is successful, otherwise returns the original value.
    """
    if value is None or value == '':
        return ''
    
    try:
        # Handle string values that might have currency symbols or other characters
        if isinstance(value, str):
            # Remove common currency symbols and whitespace
            cleaned_value = value.strip().replace('¥', '').replace('$', '').replace(',', '')
            return float(cleaned_value)
        else:
            return float(value)
    except (ValueError, TypeError):
        logging.warning(f"Could not convert '{value}' to float, returning original value")
        return value

# --- Google Sheets Helper Functions ---

def get_google_sheets_service():
    """Initializes and returns the Google Sheets API service client."""
    if not GOOGLE_CREDENTIALS_PATH:
        raise ValueError("GOOGLE_APPLICATION_CREDENTIALS environment variable not set or key file path is missing.")
    if not os.path.exists(GOOGLE_CREDENTIALS_PATH):
        raise FileNotFoundError(f"Google credentials file not found at: {GOOGLE_CREDENTIALS_PATH}")
    
    creds = service_account.Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_PATH, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    return service

def get_sheet_id_by_name(service, spreadsheet_id, sheet_name):
    """Gets the ID of a sheet by its name."""
    try:
        sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = sheet_metadata.get('sheets', [])
        for sheet in sheets:
            if sheet.get('properties', {}).get('title') == sheet_name:
                return sheet.get('properties', {}).get('sheetId')
        print(f"Warning: Sheet named '{sheet_name}' not found in spreadsheet '{spreadsheet_id}'.")
        return None # Return None if specific sheet not found
    except HttpError as error:
        print(f"An API error occurred while fetching sheet metadata for spreadsheet '{spreadsheet_id}': {error}")
        raise

def ensure_header_and_freeze(service, spreadsheet_id, sheet_id, sheet_name, header_values):
    """Ensures the header row is present, frozen, bolded, and sets specific column widths."""
    try:
        # Check current header
        range_to_check = f"{sheet_name}!1:1"
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_to_check).execute()
        current_header = result.get('values', [[]])[0]

        if current_header != header_values:
            print(f"Header mismatch or not found. Updating header in '{sheet_name}'.")
            requests_batch = [
                {
                    'updateCells': {
                        'rows': [{
                            'values': [{'userEnteredValue': {'stringValue': val}, 
                                        'userEnteredFormat': {'textFormat': {'bold': True}, 'horizontalAlignment': 'CENTER'}} 
                                       for val in header_values]
                        }],
                        'fields': 'userEnteredValue,userEnteredFormat(textFormat,horizontalAlignment)',
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 0,
                            'endRowIndex': 1,
                            'startColumnIndex': 0,
                            'endColumnIndex': len(header_values)
                        }
                    }
                },
                {
                    'updateSheetProperties': {
                        'properties': {
                            'sheetId': sheet_id,
                            'gridProperties': {'frozenRowCount': 1}
                        },
                        'fields': 'gridProperties.frozenRowCount'
                    }
                },
                {
                    'updateDimensionProperties': {
                        'range': {
                            'sheetId': sheet_id,
                            'dimension': 'COLUMNS',
                            'startIndex': 0,  # Column A (id)
                            'endIndex': 1
                        },
                        'properties': {'pixelSize': 200},
                        'fields': 'pixelSize'
                    }
                },
                {
                    'updateDimensionProperties': {
                        'range': {
                            'sheetId': sheet_id,
                            'dimension': 'COLUMNS',
                            'startIndex': 1,  # Column B (photo)
                            'endIndex': 2
                        },
                        'properties': {'pixelSize': 200},
                        'fields': 'pixelSize'
                    }
                },
                {
                    'updateDimensionProperties': { # info column (G)
                        'range': {
                            'sheetId': sheet_id,
                            'dimension': 'COLUMNS',
                            'startIndex': 6, 
                            'endIndex': 7
                        },
                        'properties': {'pixelSize': 150},
                        'fields': 'pixelSize'
                    }
                },
                {
                    'updateDimensionProperties': { # link column (H)
                        'range': {
                            'sheetId': sheet_id,
                            'dimension': 'COLUMNS',
                            'startIndex': 7, 
                            'endIndex': 8
                        },
                        'properties': {'pixelSize': 400},
                        'fields': 'pixelSize'
                    }
                }
            ]
            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id, body={'requests': requests_batch}).execute()
            print(f"Header row in '{sheet_name}' is now frozen, bolded. Columns A, B, G, H widths set.")
        else:
            print(f"Header already correct in '{sheet_name}'.")

    except HttpError as error:
        print(f"An API error occurred during header setup for sheet '{sheet_name}' in spreadsheet '{spreadsheet_id}': {error}")
        raise

def clean_url(url_string):
    parsed_url = urlparse(url_string)
    return f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"

def filter_skus_by_moq(sku_data_list, min_moq_filter, max_moq_filter, product_level_data):
    """
    Filters a list of SKU data based on MOQ criteria.
    For SKU products, we use the product-level price tier MOQs, not individual SKU MOQs.
    """
    if min_moq_filter is None and max_moq_filter is None:
        print("DEBUG: filter_skus_by_moq: No MOQ filters provided, returning original list.")
        return sku_data_list

    filtered_list = []
    product_min_order_qty_str = product_level_data.get('result', {}).get('result', {}).get('minOrderQuantity', "1")
    try:
        product_default_moq = int(product_min_order_qty_str)
    except ValueError:
        print(f"DEBUG: filter_skus_by_moq: Warning: Could not parse product minOrderQuantity '{product_min_order_qty_str}' as int. Defaulting to 1.")
        product_default_moq = 1

    print(f"DEBUG: filter_skus_by_moq: min_filter={min_moq_filter}, max_filter={max_moq_filter}, product_default_moq={product_default_moq}")

    for sku_item in sku_data_list:
        # For SKU products, use the MOQ from the product-level price tiers, not individual SKU MOQ
        item_moq_str = sku_item.get('moq')  
        current_item_moq = product_default_moq # Default to product MOQ if SKU specific cannot be determined
        
        if item_moq_str is not None:
            try:
                current_item_moq = int(item_moq_str)
            except ValueError:
                print(f"DEBUG: filter_skus_by_moq: Warning: Could not parse sku_item 'moq' ('{item_moq_str}') for SKU ID {sku_item.get('id', 'N/A')}. Using product_default_moq: {product_default_moq}.")
        else:
            print(f"DEBUG: filter_skus_by_moq: sku_item 'moq' is None for SKU ID {sku_item.get('id', 'N/A')}. Using product_default_moq: {product_default_moq}.")

        print(f"DEBUG: filter_skus_by_moq: Checking SKU ID {sku_item.get('id', 'N/A')} with effective MOQ {current_item_moq}")

        # For SKU products, if the product-level price tier MOQ meets the filter, include all SKUs
        # This is because SKUs inherit the product-level pricing structure
        if min_moq_filter is not None:
            # Check if any of the product's price tiers meet the min MOQ requirement
            meets_min_moq = current_item_moq >= min_moq_filter
            if not meets_min_moq:
                # Check if this is a low individual SKU MOQ but product has higher tier pricing
                if current_item_moq < 100:  # Likely individual SKU MOQ, check product pricing
                    print(f"DEBUG: filter_skus_by_moq: SKU has low MOQ ({current_item_moq}), checking product-level pricing compatibility")
                    meets_min_moq = True  # Allow SKUs through if they're part of a product with price tiers
                
            if not meets_min_moq:
                print(f"DEBUG: filter_skus_by_moq: SKU ID {sku_item.get('id', 'N/A')} (MOQ {current_item_moq}) failed min_moq_filter ({min_moq_filter}). Skipping.")
                continue 
        
        # Apply max_moq filter
        if max_moq_filter is not None and current_item_moq > max_moq_filter:
            print(f"DEBUG: filter_skus_by_moq: SKU ID {sku_item.get('id', 'N/A')} (MOQ {current_item_moq}) failed max_moq_filter ({max_moq_filter}). Skipping.")
            continue 
            
        filtered_list.append(sku_item)
        print(f"DEBUG: filter_skus_by_moq: SKU ID {sku_item.get('id', 'N/A')} (MOQ {current_item_moq}) passed filters. Added to list.")
        
    return filtered_list

def get_material_info(product_data_full_api_response, product_level_attributes_dict, sku_specific_attributes_list=None):
    """
    Extracts material information, prioritizing SKU-specific attributes then product-level.
    '287' is the assumed attributeId for material.
    Returns: (material_string, source_string) where source is 'sku', 'product', or 'none'.
    """
    material = ""
    material_source = "none"

    # 1. Check SKU-specific attributes if provided
    if sku_specific_attributes_list:
        for attr in sku_specific_attributes_list:
            if isinstance(attr, dict) and str(attr.get('attributeId')) == '287':
                material = attr.get('valueTrans', attr.get('value', ''))
                if material:
                    material_source = "sku"
                    # print(f"DEBUG get_material_info: Found material '{material}' from SKU attributes.")
                    return material, material_source

    # 2. Fallback to product-level attributes dictionary
    if isinstance(product_level_attributes_dict, dict) and '287' in product_level_attributes_dict:
        material = product_level_attributes_dict['287']
        if material:
            material_source = "product"
            # print(f"DEBUG get_material_info: Found material '{material}' from product attributes.")
            return material, material_source
    
    # print(f"DEBUG get_material_info: No material found. Defaulting to: '{material}', source: '{material_source}'.")
    return material, material_source

def process_and_upload_data(service, spreadsheet_id, sheet_name, product_data_path, product_type, min_moq, max_moq, sheet_id_val, source_url):
    """Process product data and upload to Google Sheets with detailed logging."""
    logger.info(f"Starting processing for product data: {product_data_path}")
    logger.debug(f"Parameters - sheet_name: {sheet_name}, product_type: {product_type}, min_moq: {min_moq}, max_moq: {max_moq}")
    logger.info(f"--- Starting Google Sheet Update for Sheet ID: {spreadsheet_id} ---")
    logger.info(f"Product Type: {product_type}, Min MOQ: {min_moq}, Max MOQ: {max_moq}")
    logger.info(f"Source URL: {source_url}, Data Path: {product_data_path}")

    cleaned_source_url = clean_url(source_url) # Define cleaned_source_url

    try:
        with open(product_data_path, 'r') as f:
            data = json.load(f)

        # Extract product name early for logging
        product_name = data.get('result', {}).get('result', {}).get('subjectTrans', 
                      data.get('result', {}).get('result', {}).get('subject', 'Unknown Product'))
        product_name_short = product_name[:40] + "..." if len(product_name) > 40 else product_name
        
        print(f"📦 Processing product: {product_name_short}")

        # Debug: Log the structure of the data
        print("\n--- DEBUG: API Response Structure ---")
        print(f"Top-level keys: {list(data.keys())}")
        
        # Check if the API returned an error
        if 'status' in data and data.get('status') != 'success':
            error_msg = data.get('message', 'No error message provided')
            print(f"❌ API Error: {error_msg}")
            print(f"Full response: {json.dumps(data, indent=2, ensure_ascii=False)}")
            print("----------------------------------\n")
            return {
                "product_name": product_name_short,
                "rows_uploaded": 0,
                "skus_found": 0,
                "skus_after_filter": 0,
                "price_tiers_count": 0,
                "error": error_msg
            }
            
        if 'result' in data:
            print(f"Result keys: {list(data['result'].keys()) if isinstance(data['result'], dict) else 'Not a dict'}")
            if isinstance(data['result'], dict) and 'result' in data['result']:
                print(f"Nested result keys: {list(data['result']['result'].keys()) if isinstance(data['result']['result'], dict) else 'Not a dict'}")
        print("----------------------------------\n")

        # Store the full product data for later use
        product_data = data
        
        # Try different possible locations for SKU info
        product_sku_infos = []
        result_data = {}
        
        # Extract result data if it exists
        if 'result' in data and isinstance(data['result'], dict):
            result_data = data['result'].get('result', {})
            print("Found result data in response")
        
        # Log all available keys for debugging
        print(f"Available top-level keys: {list(data.keys())}")
        if 'result' in data and isinstance(data['result'], dict):
            print(f"Result keys: {list(data['result'].keys())}")
            if 'result' in data['result'] and isinstance(data['result']['result'], dict):
                print(f"Nested result keys: {list(data['result']['result'].keys())}")
        
        # Try different locations for SKU information
        possible_sku_locations = [
            result_data.get('productSkuInfos'),
            result_data.get('skuList'),
            result_data.get('productInfo', {}).get('skuList') if isinstance(result_data.get('productInfo'), dict) else None,
            result_data.get('productInfo', {}).get('productSkuInfos') if isinstance(result_data.get('productInfo'), dict) else None,
            data.get('result', {}).get('skuList'),
            data.get('skuList')
        ]
        
        # Find the first non-None SKU list
        for sku_list in possible_sku_locations:
            if isinstance(sku_list, list) and len(sku_list) > 0:
                product_sku_infos = sku_list
                print(f"🔍 Found {len(sku_list)} SKUs in API response")
                break
        
        print(f"DEBUG: product_sku_infos after find_sku_data: {product_sku_infos} (type: {type(product_sku_infos)}, len: {len(product_sku_infos) if product_sku_infos else 0})")

        # If product_sku_infos is still empty after checking all locations, then print the warning and potentially return.
        if not product_sku_infos:
            logger.debug("No SKUs found after checking all possible locations.")
            logger.warning("No product SKU information found in the JSON data.")
            logger.debug("Attempted to find SKUs in these locations:")
            logger.debug("1. result.result.productSkuInfos")
            logger.debug("2. result.result.skuList")
            logger.debug("3. result.result.productInfo.skuList")
            logger.debug("4. result.result.productInfo.productSkuInfos")
            logger.debug("5. result.skuList")
            logger.debug("6. skuList")
            logger.debug("\nAvailable data structure:")
            logger.debug(json.dumps(data, indent=2, ensure_ascii=False)[:1000] + '...')
            # Decide if we should return or proceed with product-level data if SKUs are truly absent.
            # For now, the logic below will handle creating a product-level entry if product_sku_infos remains empty.

        # --- Determine last_id_row (last row with an ID, or header_row_index if no data) ---
        header_row_index = 1 
        last_id_row = header_row_index 
        try:
            range_to_check = f'{sheet_name}!A{header_row_index + 1}:A'
            logger.debug(f"Reading ID column from range: {range_to_check} to determine last_id_row.")
            result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_to_check).execute()
            id_column_values = result.get('values', [])

            if id_column_values:
                found_actual_last_id = False
                for i in range(len(id_column_values) - 1, -1, -1):
                    if id_column_values[i] and len(id_column_values[i]) > 0 and str(id_column_values[i][0]).strip():
                        last_id_row = (header_row_index + 1) + i # (header_row_index + 1) is the first data row, i is 0-indexed from there
                        found_actual_last_id = True
                        break
                if not found_actual_last_id:
                    logger.debug(f"No valid IDs found in column A after header row {header_row_index}. last_id_row remains {last_id_row} (header row).")
            else:
                logger.debug(f"ID column A from row {header_row_index + 1} onwards is empty. last_id_row remains {last_id_row} (header row).")
            
            logger.info(f"Determined last_id_row (last row with data, or header row if empty): {last_id_row} (Sheet: '{sheet_name}')")

        except Exception as e:
            logger.warning(f"Error reading ID column to determine last_id_row for sheet '{sheet_name}'. Defaulting to {last_id_row} (header row). Error: {e}")

        # We'll use a Google Sheets formula for ID generation instead of backend logic
        # This ensures consecutive numbering regardless of deletions or gaps
        current_date = datetime.datetime.now().strftime('%Y%m%d')
        logger.info(f"Using date {current_date} for ID formula in Google Sheets")

        main_product_image = ""
        if product_data.get('result', {}).get('result', {}).get('productImage', {}).get('images') and \
           isinstance(product_data['result']['result']['productImage']['images'], list) and \
           len(product_data['result']['result']['productImage']['images']) > 0:
            main_product_image = product_data['result']['result']['productImage']['images'][0]
        logger.debug(f"main_product_image: {main_product_image}")

        product_attributes = {}
        if product_data.get('result', {}).get('result', {}).get('productAttribute'):
            for attr in product_data['result']['result']['productAttribute']:
                if isinstance(attr, dict) and 'attributeId' in attr and 'valueTrans' in attr:
                    product_attributes[str(attr['attributeId'])] = str(attr['valueTrans'])
        logger.debug(f"product_attributes: {product_attributes}")

        # --- MOQ Filtering for priceRangeList to create price_tiers_to_process --- 
        price_tiers_to_process = []
        original_price_range_list = product_data.get('result', {}).get('result', {}).get('productSaleInfo', {}).get('priceRangeList', []) 
        logger.debug(f"Original productSaleInfo.priceRangeList: {original_price_range_list}")

        temp_parsed_price_ranges = []
        for tier in original_price_range_list:
            try:
                tier_copy = tier.copy()
                tier_copy['startQuantity'] = int(tier_copy['startQuantity'])
                temp_parsed_price_ranges.append(tier_copy)
            except (ValueError, TypeError, KeyError):
                logger.debug(f"Skipping tier in priceRangeList due to invalid 'startQuantity': {tier}")
                continue
        
        parsed_price_ranges = sorted(temp_parsed_price_ranges, key=lambda t: t.get('startQuantity', float('inf')))
        logger.debug(f"Parsed and sorted price_ranges (from productSaleInfo): {parsed_price_ranges}")

        list_after_min_filter = []
        if min_moq is not None:
            s_active = None
            for i, current_tier_data in enumerate(parsed_price_ranges):
                current_tier_start_val = current_tier_data['startQuantity']
                next_tier_start_val = parsed_price_ranges[i+1]['startQuantity'] if i + 1 < len(parsed_price_ranges) else float('inf')
                if current_tier_start_val <= min_moq < next_tier_start_val:
                    s_active = current_tier_start_val
                    break
            if s_active is None and parsed_price_ranges and min_moq < parsed_price_ranges[0]['startQuantity']:
                s_active = parsed_price_ranges[0]['startQuantity'] 
            if s_active is not None:
                for tier_data_to_filter in parsed_price_ranges:
                    if tier_data_to_filter['startQuantity'] >= s_active:
                        list_after_min_filter.append(tier_data_to_filter)
        else:
            list_after_min_filter = list(parsed_price_ranges)
        logger.debug(f"Price tiers after min_moq ({min_moq}) filter: {list_after_min_filter}")

        filtered_price_range_list_final = []
        if max_moq is not None:
            for tier in list_after_min_filter:
                if tier['startQuantity'] <= max_moq:
                    filtered_price_range_list_final.append(tier)
        else:
            filtered_price_range_list_final = list(list_after_min_filter)
        logger.debug(f"Price tiers after max_moq ({max_moq}) filter: {filtered_price_range_list_final}")

        if filtered_price_range_list_final:
            for tier in filtered_price_range_list_final:
                price_tiers_to_process.append({
                    'moq': str(tier.get('startQuantity', '')),
                    'price1688': str(tier.get('price', ''))
                })
        else:
            # If no tiers match, or original list was empty, use product's direct price and minOrderQuantity if available
            product_direct_price = str(product_data.get('result', {}).get('result', {}).get('productSaleInfo', {}).get('price', ''))
            product_min_order_qty = str(product_data.get('result', {}).get('result', {}).get('minOrderQuantity', ''))
            if product_direct_price and product_min_order_qty:
                logger.debug(f"No price tiers matched MOQ. Using product direct price {product_direct_price} and minOrderQuantity {product_min_order_qty} as a fallback tier.")
                price_tiers_to_process.append({'moq': product_min_order_qty, 'price1688': product_direct_price})
            else:
                logger.debug("No price tiers matched MOQ, and no product direct price/minOrderQuantity for fallback. Adding default empty tier.")
                price_tiers_to_process.append({'moq': '', 'price1688': ''})
        logger.debug(f"Final price_tiers_to_process after MOQ filtering and fallbacks: {price_tiers_to_process}")

        # --- Populate sku_data --- 
        logger.debug(f"Before 'if not product_sku_infos' check. product_sku_infos is: {product_sku_infos} (len: {len(product_sku_infos) if product_sku_infos else 0})")

        sku_data_final_rows = []

        if not product_sku_infos: # Case: No SKUs found, use product-level data
            logger.debug("Entered 'if not product_sku_infos' block (product-level data path).")
            
            # Define product_default_moq for the no-SKU path
            product_min_order_qty_str = data.get('result', {}).get('result', {}).get('minOrderQuantity', "1")
            try:
                product_default_moq = int(product_min_order_qty_str)
            except ValueError:
                logger.warning(f"Could not parse product minOrderQuantity '{product_min_order_qty_str}' for no-SKU path. Defaulting to 1.")
                product_default_moq = 1

            # Define main_product_image_formula here for the no-SKU path, before the loop
            main_product_image_formula = f'=HYPERLINK("{main_product_image}", IMAGE("{main_product_image}"))' if main_product_image else ""

            if not price_tiers_to_process: 
                logger.error(f"No data found in {product_data_path} or file is empty. Cannot determine data to upload.")
            else:
                logger.debug(f"DEBUG: About to process {len(price_tiers_to_process)} price tiers.")
                for tier_index, tier in enumerate(price_tiers_to_process):
                    logger.debug(f"DEBUG: Processing tier {tier_index}")
                    
                    product_info_str = data.get('result', {}).get('result', {}).get('subjectTrans', data.get('result', {}).get('result', {}).get('subject', 'N/A'))
                    material, material_source = get_material_info(data, product_attributes)

                    row_data = {
                        'image': main_product_image_formula, # Now defined
                        'price1688': tier.get('price1688', 'N/A'),
                        'moq': tier.get('moq', str(product_default_moq)), # Now defined
                        'info': product_info_str, 
                        'material': material,
                        'link': cleaned_source_url
                    }
                    sku_data_final_rows.append(row_data)
                print(f"DEBUG: sku_data_final_rows after processing product-level tiers: {sku_data_final_rows}")

        else: # Case: SKUs exist
            print(f"DEBUG: Entered 'else' block (SKUs exist path). Processing {len(product_sku_infos)} SKUs.")
            # Determine product_default_moq once for SKU path
            product_min_order_qty_str = data.get('result', {}).get('result', {}).get('minOrderQuantity', "1")
            try:
                product_default_moq = int(product_min_order_qty_str)
            except ValueError:
                print(f"Warning: Could not parse product minOrderQuantity '{product_min_order_qty_str}' as int. Defaulting to 1.")
                product_default_moq = 1

            for sku_index, sku in enumerate(product_sku_infos):
                logger.debug(f"DEBUG: Processing SKU {sku_index}")

                sku_attributes_list = sku.get('skuAttributes', [])
                sku_info_parts = []
                sku_image_url_for_sku = main_product_image # Fallback to main product image for this SKU

                for attr in sku_attributes_list:
                    attr_name = attr.get('attributeNameTrans', attr.get('attributeName', 'N/A'))
                    attr_value = attr.get('valueTrans', attr.get('value', 'N/A'))
                    sku_info_parts.append(f"{attr_name}: {attr_value}")
                    if attr.get('skuImageUrl'): # If SKU has specific image, use it
                        sku_image_url_for_sku = attr.get('skuImageUrl')
                
                sku_info_str = ", ".join(sku_info_parts) if sku_info_parts else "N/A"
                image_formula = f'=HYPERLINK("{sku_image_url_for_sku}", IMAGE("{sku_image_url_for_sku}"))' if sku_image_url_for_sku else ""
                
                sku_price = sku.get('price')
                if sku_price is None:
                    print(f"DEBUG: SKU {sku.get('skuId', 'N/A')} has no price, skipping.")
                    continue

                material, material_source = get_material_info(data, product_attributes, sku_attributes_list)

                row_data = {
                    'image': image_formula,
                    'price1688': str(sku_price),  # Use SKU's own price
                    'moq': str(product_default_moq),  # Use product's default MOQ
                    'info': sku_info_str,
                    'material': material,
                    'link': cleaned_source_url
                }
                sku_data_final_rows.append(row_data)
            
            print(f"DEBUG: sku_data after processing all SKUs: {sku_data_final_rows}")

            # Fallback for SKUs with no individual price: repeat SKUs for each product-level price tier
            if not sku_data_final_rows and price_tiers_to_process:
                logger.debug("DEBUG: No SKU-level price data found. Falling back to repeating SKUs for each product-level price tier.")
                fallback_rows = []
                for tier in price_tiers_to_process:
                    for sku in product_sku_infos:
                        logger.debug(f"DEBUG: Fallback processing tier {tier} and SKU {sku.get('skuId')}")
                        # build sku_info_str and image formula
                        sku_attributes_list = sku.get('skuAttributes', [])
                        sku_info_parts = []
                        sku_image_url_for_sku = main_product_image
                        for attr in sku_attributes_list:
                            attr_name = attr.get('attributeNameTrans', attr.get('attributeName', 'N/A'))
                            attr_value = attr.get('valueTrans', attr.get('value', 'N/A'))
                            sku_info_parts.append(f"{attr_name}: {attr_value}")
                            if attr.get('skuImageUrl'):
                                sku_image_url_for_sku = attr.get('skuImageUrl')
                        sku_info_str = ", ".join(sku_info_parts) if sku_info_parts else "N/A"
                        image_formula = f'=HYPERLINK("{sku_image_url_for_sku}", IMAGE("{sku_image_url_for_sku}"))' if sku_image_url_for_sku else ""
                        # use tier price and moq
                        material, _ = get_material_info(data, product_attributes, sku_attributes_list)
                        row_data = {
                            'image': image_formula,
                            'price1688': tier.get('price1688', 'N/A'),
                            'moq': str(tier.get('moq', product_default_moq)),
                            'info': sku_info_str,
                            'material': material,
                            'link': cleaned_source_url
                        }
                        fallback_rows.append(row_data)
                sku_data_final_rows = fallback_rows
                print(f"DEBUG: sku_data after fallback processing: {sku_data_final_rows}")

        # Filter the collected sku_data_final_rows by MOQ (user-defined filters)
        if min_moq is not None or max_moq is not None:
            print(f"DEBUG: Before SKU-level MOQ filtering. min_moq: {min_moq}, max_moq: {max_moq}. Current sku_data: {sku_data_final_rows}")
            sku_data_final_rows = filter_skus_by_moq(sku_data_final_rows, min_moq, max_moq, data) # Pass 'data' for product_level_data
            print(f"✅ After MOQ filtering: {len(sku_data_final_rows)} SKUs remaining (from {len(product_sku_infos) if product_sku_infos else 0} original)")
        else:
            print(f"DEBUG: SKU-level MOQ filtering skipped (no SKUs to filter or no MOQ params).")

        if not sku_data_final_rows:
            print("DEBUG: sku_data_final_rows is empty after all processing and filtering. No data to upload.")
            print("No SKUs found after filtering by MOQ or no initial product/SKU data.")
            return {
                "product_name": product_name_short,
                "rows_uploaded": 0,
                "skus_found": len(product_sku_infos) if product_sku_infos else 0,
                "skus_after_filter": 0,
                "price_tiers_count": len(price_tiers_to_process) if 'price_tiers_to_process' in locals() else 0,
                "error": "No SKUs found after filtering by MOQ or no initial product/SKU data."
            }

        # --- Second pass: create rows_to_append based on sku_data_final_rows --- 
        rows_to_append = []
        price_moq_groups = []
        
        if not sku_data_final_rows:
            logging.info("sku_data_final_rows is empty. No data will be appended to the sheet.")
            # Ensure last_id_row is defined even if no data is appended, for consistency
            # if 'last_id_row' not in locals(): # This check might be redundant now
            #     last_id_row = header_row_index # Default if not determined earlier
        else:
            logging.debug(f"Before final row generation. sku_data_final_rows has {len(sku_data_final_rows)} items: {sku_data_final_rows[:3]}") # Log first 3 for brevity
            for item_data in sku_data_final_rows:
                price_1688 = try_convert_to_float(item_data.get('price1688', ''))
                price_cust = round(price_1688 * 1.15, 1) if price_1688 != '' else ''
                
                # Create formula for consecutive ID generation
                id_formula = f'="{current_date}_{product_type}_" & TEXT(ROW()-1,"000")'
                
                row = [
                    id_formula,  # Google Sheets formula for consecutive IDs
                    item_data.get('image', ''),
                    price_1688,
                    price_cust,  # Calculated as price_1688 * 1.15, rounded to 1 decimal
                    '',  # Placeholder for 'Landed Cost'
                    item_data.get('moq', ''),
                    item_data.get('info', ''),
                    item_data.get('material', ''),
                    item_data.get('link', '')
                ]
                rows_to_append.append(row)
            logging.debug(f"Generated {len(rows_to_append)} rows for rows_to_append. First 3: {rows_to_append[:3]}")
            
            # Rebuild price_moq_groups based on the final rows_to_append and their MOQs
            # This is critical for correct merging if rows_to_append was modified (e.g., by filtering)
            current_moq = None
            current_count = 0
            start_idx_in_batch = 0 # This will be relative to the start of the current batch being appended
            
            for i, row_data in enumerate(rows_to_append):
                moq_in_row = row_data[5] # Assuming MOQ is at index 5
                if moq_in_row != current_moq:
                    if current_moq is not None: # Save the previous group
                        end_row = start_idx_in_batch + current_count - 1
                        price_moq_groups.append({
                            'moq': current_moq, 
                            'count': current_count, 
                            'start_idx_in_batch': start_idx_in_batch,
                            'start_row': start_idx_in_batch,
                            'end_row': end_row
                        })
                    current_moq = moq_in_row
                    current_count = 1
                    start_idx_in_batch = i 
                else:
                    current_count += 1
            
            if current_moq is not None: # Save the last group
                end_row = start_idx_in_batch + current_count - 1
                price_moq_groups.append({
                    'moq': current_moq, 
                    'count': current_count, 
                    'start_idx_in_batch': start_idx_in_batch,
                    'start_row': start_idx_in_batch,
                    'end_row': end_row
                })
            logging.debug(f"price_moq_groups for merging: {price_moq_groups}")

        if rows_to_append:
            # Determine the starting row for appending new data.
            # This should be the first empty row after the last data row.
            # last_id_row is the row number of the last data entry, or header_row_index if no data exists.
            start_row_for_new_data = last_id_row + 1
            
            append_range = f'{sheet_name}!A{start_row_for_new_data}'
            logging.info(f"Appending {len(rows_to_append)} rows to range: {append_range}")
        else:
            print("DEBUG: rows_to_append is empty after trying to generate rows. No data will be uploaded.")
            print("No data processed to upload (rows_to_append is empty).")
            return {
                "product_name": product_name_short,
                "rows_uploaded": 0,
                "skus_found": len(product_sku_infos) if product_sku_infos else 0,
                "skus_after_filter": 0,
                "price_tiers_count": len(price_tiers_to_process) if 'price_tiers_to_process' in locals() else 0,
                "error": "No data processed to upload (rows_to_append is empty)."
            }

        # Append data after the header (assuming header is row 1)
        # The append operation will add data starting at the first empty row it finds.
        body = {'values': rows_to_append}
        append_result = service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id, range=f"{sheet_name}!A2", # Start appending from row 2
            valueInputOption='USER_ENTERED',
            insertDataOption='INSERT_ROWS',
            body=body,
            includeValuesInResponse=False # Not needed, but good to be explicit
        ).execute()
        
        print(f"🎉 Successfully uploaded {len(rows_to_append)} product variants to Google Sheet")
        print(f"📊 Summary for '{product_name_short}': {len(product_sku_infos) if product_sku_infos else 0} SKUs found → {len(sku_data_final_rows)} after filtering → {len(rows_to_append)} uploaded")

        # Set row heights for the newly added rows
        updated_range_str = append_result.get('updates', {}).get('updatedRange', '')
        if updated_range_str:
            try:
                range_parts = updated_range_str.split('!')[-1]
                start_cell, _ = range_parts.split(':') # We only need the start cell for the initial row index
                
                import re
                start_row_match = re.search(r'\d+', start_cell)

                if start_row_match:
                    actual_start_row_1_indexed = int(start_row_match.group(0))

                    # --- Apply Cell Merges for MOQ within price/moq groups ---
                    merge_requests = []
                    
                    for group in price_moq_groups:
                        if group['end_row'] > group['start_row']:  # If there's more than one row in this group
                            # Merge the MOQ column (index 5, column F) for each price/moq group
                            merge_requests.append({
                                'mergeCells': {
                                    'range': {
                                        'sheetId': sheet_id_val,
                                        'startRowIndex': actual_start_row_1_indexed - 1 + group['start_idx_in_batch'],
                                        'endRowIndex': actual_start_row_1_indexed - 1 + group['start_idx_in_batch'] + group['count'],
                                        'startColumnIndex': 5,  # MOQ column (F)
                                        'endColumnIndex': 6     # End at column G (exclusive)
                                    },
                                    'mergeType': 'MERGE_ALL'
                                }
                            })
                            
                            # Check if we should merge material cells (only if material is from product level)
                            should_merge_material = all(
                                sku_data_final_rows[i].get('material_source') == 'product' 
                                for i in range(group['start_idx_in_batch'], group['start_idx_in_batch'] + group['count'])
                                if i < len(sku_data_final_rows)
                            )
                            
                            if should_merge_material:
                                merge_requests.append({
                                    'mergeCells': {
                                        'range': {
                                            'sheetId': sheet_id_val,
                                            'startRowIndex': actual_start_row_1_indexed - 1 + group['start_idx_in_batch'],
                                            'endRowIndex': actual_start_row_1_indexed - 1 + group['start_idx_in_batch'] + group['count'],
                                            'startColumnIndex': 7,  # Material column (H)
                                            'endColumnIndex': 8     # End at column I (exclusive)
                                        },
                                        'mergeType': 'MERGE_ALL'
                                    }
                                })
                            
                            # Also merge the link column (index 8, column I) for each price/moq group
                            merge_requests.append({
                                'mergeCells': {
                                    'range': {
                                        'sheetId': sheet_id_val,
                                        'startRowIndex': actual_start_row_1_indexed - 1 + group['start_idx_in_batch'],
                                        'endRowIndex': actual_start_row_1_indexed - 1 + group['start_idx_in_batch'] + group['count'],
                                        'startColumnIndex': 8,  # Link column (I)
                                        'endColumnIndex': 9     # End at column J (exclusive)
                                    },
                                    'mergeType': 'MERGE_ALL'
                                }
                            })
                    
                    if merge_requests:
                        service.spreadsheets().batchUpdate(
                            spreadsheetId=spreadsheet_id, body={'requests': merge_requests}).execute()
                        print(f"Successfully merged MOQ cells for {len(merge_requests)} price/moq groups.")
                    # --- End Cell Merges ---

                    # --- Set Row Height to 200px for each SKU ---
                    row_height_requests = []
                    TARGET_ROW_HEIGHT = 200  # 200px per SKU row
                    
                    # Apply height to all rows
                    row_height_requests.append({
                        'updateDimensionProperties': {
                            'range': {
                                'sheetId': sheet_id_val,
                                'dimension': 'ROWS',
                                'startIndex': actual_start_row_1_indexed - 1,
                                'endIndex': actual_start_row_1_indexed - 1 + len(rows_to_append)
                            },
                            'properties': {'pixelSize': TARGET_ROW_HEIGHT},
                            'fields': 'pixelSize'
                        }
                    })
                    
                    # Set column widths for material and link columns
                    column_width_requests = []
                    
                    # Material column (H, index 7) - 100px
                    column_width_requests.append({
                        'updateDimensionProperties': {
                            'range': {
                                'sheetId': sheet_id_val,
                                'dimension': 'COLUMNS',
                                'startIndex': 7,  # Column H (material)
                                'endIndex': 8
                            },
                            'properties': {'pixelSize': 100},
                            'fields': 'pixelSize'
                        }
                    })
                    
                    # Link column (I, index 8) - 300px
                    column_width_requests.append({
                        'updateDimensionProperties': {
                            'range': {
                                'sheetId': sheet_id_val,
                                'dimension': 'COLUMNS',
                                'startIndex': 8,  # Column I (link)
                                'endIndex': 9
                            },
                            'properties': {'pixelSize': 300},
                            'fields': 'pixelSize'
                        }
                    })
                    
                    if row_height_requests:
                        service.spreadsheets().batchUpdate(
                            spreadsheetId=spreadsheet_id, body={'requests': row_height_requests}).execute()
                        print(f"Set consistent row height of {TARGET_ROW_HEIGHT}px for all rows.")
                    
                    if column_width_requests:
                        service.spreadsheets().batchUpdate(
                            spreadsheetId=spreadsheet_id, body={'requests': column_width_requests}).execute()
                        print("Set column widths: Material=100px, Link=300px.")
                    # --- Apply Final Cell Formatting (Alignment, Currency, Formulas) ---
                    formatting_requests = []
                    actual_end_row_1_indexed = (actual_start_row_1_indexed - 1) + len(rows_to_append)

                    # 0. Set default formatting for all cells (centered, not bold, wrapped text)
                    num_columns = len(EXPECTED_HEADER)  # Dynamically get number of columns
                    formatting_requests.append({
                        'repeatCell': {
                            'range': {
                                'sheetId': sheet_id_val,
                                'startRowIndex': actual_start_row_1_indexed - 1,
                                'endRowIndex': actual_end_row_1_indexed,
                                'startColumnIndex': 0,  # Column A
                                'endColumnIndex': num_columns  # All columns dynamically
                            },
                            'cell': {
                                'userEnteredFormat': {
                                    'horizontalAlignment': 'CENTER',
                                    'verticalAlignment': 'MIDDLE',
                                    'wrapStrategy': 'WRAP',
                                    'textFormat': {'bold': False}
                                }
                            },
                            'fields': 'userEnteredFormat(horizontalAlignment,verticalAlignment,wrapStrategy,textFormat.bold)'
                        }
                    })

                    # 1. Format 'price 1688' (Column C, index 2) and 'price cust' (Column D, index 3) as CNY
                    for col_idx in [2, 3]:  # Columns C and D (0-based index 2 and 3)
                        formatting_requests.append({
                            'repeatCell': {
                                'range': {
                                    'sheetId': sheet_id_val,
                                    'startRowIndex': actual_start_row_1_indexed - 1,
                                    'endRowIndex': actual_end_row_1_indexed,
                                    'startColumnIndex': col_idx,
                                    'endColumnIndex': col_idx + 1
                                },
                                'cell': {
                                    'userEnteredFormat': {
                                        'numberFormat': {
                                            'type': 'CURRENCY',
                                            'pattern': '¥#,##0.00;¥-#,##0.00'
                                        },
                                        'horizontalAlignment': 'CENTER',
                                        'verticalAlignment': 'MIDDLE'
                                    }
                                },
                                'fields': 'userEnteredFormat(numberFormat,horizontalAlignment,verticalAlignment)'
                            }
                        })

                    # 2. Set Profit Formulas (Column E, index 4)
                    for i in range(len(rows_to_append)):
                        current_row = actual_start_row_1_indexed + i
                        formula = f'=IF(AND(ISNUMBER(D{current_row}),ISNUMBER(C{current_row})),D{current_row}-C{current_row},"")'
                        formatting_requests.append({
                            'updateCells': {
                                'range': {
                                    'sheetId': sheet_id_val,
                                    'startRowIndex': current_row - 1,
                                    'endRowIndex': current_row,
                                    'startColumnIndex': 4,  # Column E (profit)
                                    'endColumnIndex': 5
                                },
                                'rows': [{
                                    'values': [{
                                        'userEnteredValue': {'formulaValue': formula},
                                        'userEnteredFormat': {
                                            'numberFormat': {
                                                'type': 'CURRENCY',
                                                'pattern': '¥#,##0.00;¥-#,##0.00'
                                            },
                                            'horizontalAlignment': 'CENTER',
                                            'verticalAlignment': 'MIDDLE'
                                        }
                                    }]
                                }],
                                'fields': 'userEnteredValue,userEnteredFormat(numberFormat,horizontalAlignment,verticalAlignment)'
                            }
                        })
                    
                    # 3. Apply all formatting requests
                    if formatting_requests:
                        service.spreadsheets().batchUpdate(
                            spreadsheetId=spreadsheet_id,
                            body={'requests': formatting_requests}
                        ).execute()
                        print("Applied cell formatting and formulas.")
                    # --- End Cell Formatting ---

                    # 4. Set Text Wrapping for 'info' column (G, index 6)
                    formatting_requests.append({
                        'repeatCell': {
                            'range': {
                                'sheetId': sheet_id_val,
                                'startRowIndex': actual_start_row_1_indexed - 1,
                                'endRowIndex': actual_end_row_1_indexed,
                                'startColumnIndex': 6, # Column G
                                'endColumnIndex': 7
                            },
                            'cell': {
                                'userEnteredFormat': {'wrapStrategy': 'WRAP'}
                            },
                            'fields': 'userEnteredFormat.wrapStrategy'
                        }
                    })

                    # 6. Set Text Wrapping for 'link' column (H, index 7)
                    formatting_requests.append({
                        'repeatCell': {
                            'range': {
                                'sheetId': sheet_id_val,
                                'startRowIndex': actual_start_row_1_indexed - 1,
                                'endRowIndex': actual_end_row_1_indexed,
                                'startColumnIndex': 7, # Column H
                                'endColumnIndex': 8
                            },
                            'cell': {
                                'userEnteredFormat': {'wrapStrategy': 'WRAP'}
                            },
                            'fields': 'userEnteredFormat.wrapStrategy'
                        }
                    })

                    if formatting_requests:
                        service.spreadsheets().batchUpdate(
                            spreadsheetId=spreadsheet_id, body={'requests': formatting_requests}).execute()
                        print("Applied final cell formatting: alignment, currency, and profit formulas.")
                    # --- End Final Cell Formatting ---

                else:
                    print(f"Could not parse row numbers from updatedRange: {updated_range_str}")
            except Exception as e:
                print(f"Error processing formatting for updatedRange '{updated_range_str}': {e}")
        else:
            print("Could not determine updated range to set row heights.")

    except HttpError as error:
        logger.error(f"An API error occurred during data upload: {error}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred during data processing: {e}", exc_info=True)
        raise

    return {
        "product_name": product_name_short,
        "rows_uploaded": len(rows_to_append),
        "skus_found": len(product_sku_infos) if product_sku_infos else 0,
        "skus_after_filter": len(sku_data_final_rows),
        "price_tiers_count": len(price_tiers_to_process) if 'price_tiers_to_process' in locals() else 0,
        "price_moq_groups": price_moq_groups
    }

def run_sheet_update(product_data_path, product_type_arg, min_moq_arg, max_moq_arg, source_url_arg, google_sheet_id_param):
    """Main function to orchestrate the sheet update process using a provided Google Sheet ID."""
    
    if not google_sheet_id_param:
        error_msg = "Google Sheet ID is required but was not provided to run_sheet_update."
        logger.error(f"Error processing data: {str(error_msg)}")
        raise ValueError(error_msg)

    logger.info(f"--- Starting Google Sheet Update for Sheet ID: {google_sheet_id_param} ---")
    logger.info(f"Product Type: {product_type_arg}, Min MOQ: {min_moq_arg}, Max MOQ: {max_moq_arg}")
    logger.info(f"Source URL: {source_url_arg}, Data Path: {product_data_path}")

    try:
        service = get_google_sheets_service()
        logger.info("Google Sheets service initialized successfully.")

        sheet_id_val = get_sheet_id_by_name(service, google_sheet_id_param, TARGET_SHEET_NAME)

        if sheet_id_val is None:
            if TARGET_SHEET_NAME:
                logger.info(f"Sheet named '{TARGET_SHEET_NAME}' not found in spreadsheet '{google_sheet_id_param}'. Attempting to create it.")
                try:
                    add_sheet_request_body = {
                        'requests': [{
                            'addSheet': {
                                'properties': {
                                    'title': TARGET_SHEET_NAME
                                }
                            }
                        }]
                    }
                    response = service.spreadsheets().batchUpdate(
                        spreadsheetId=google_sheet_id_param, 
                        body=add_sheet_request_body
                    ).execute()
                    new_sheet_properties = response.get('replies')[0].get('addSheet').get('properties')
                    sheet_id_val = new_sheet_properties.get('sheetId')
                    logger.info(f"Successfully created sheet '{TARGET_SHEET_NAME}' with ID: {sheet_id_val}")
                except HttpError as error_create:
                    err_msg = f"Failed to create sheet '{TARGET_SHEET_NAME}' in spreadsheet '{google_sheet_id_param}': {error_create}"
                    logger.error(err_msg)
                    raise ValueError(err_msg)
            else:
                err_msg = f"Target sheet name ('{TARGET_SHEET_NAME}') not found in spreadsheet '{google_sheet_id_param}' and no sheet name configured to create."
                logger.error(err_msg)
                raise ValueError(err_msg)

        ensure_header_and_freeze(service, google_sheet_id_param, sheet_id_val, TARGET_SHEET_NAME, EXPECTED_HEADER)
        logger.info(f"Header check/update for sheet '{TARGET_SHEET_NAME}' (ID: {sheet_id_val}) complete.")

        # Get statistics from the data processing
        stats = process_and_upload_data(
            service, 
            google_sheet_id_param, 
            TARGET_SHEET_NAME, 
            product_data_path, 
            product_type_arg, 
            min_moq_arg, 
            max_moq_arg, 
            sheet_id_val, 
            source_url_arg
        )
        logger.info(f"Data processing and upload for sheet '{TARGET_SHEET_NAME}' complete.")
        logger.info(f"--- Google Sheet Update for Sheet ID: {google_sheet_id_param} Finished Successfully ---")
        
        return stats  # Return the statistics

    except FileNotFoundError as e_fnf:
        logger.error(f"File not found during sheet update for '{google_sheet_id_param}': {e_fnf}")
        raise
    except ValueError as e_val:
        logger.error(f"Value error during sheet update for '{google_sheet_id_param}': {e_val}")
        raise
    except HttpError as e_http:
        logger.error(f"API error during sheet update for '{google_sheet_id_param}': {e_http}")
        logger.error(f"Details: {e_http.content}")
        raise
    except Exception as e_general:
        logger.error(f"Unexpected error during sheet update for '{google_sheet_id_param}': {e_general}", exc_info=True)
        raise

if __name__ == '__main__':
    # This part is now more for structure; direct execution might require specific args.
    # For full functionality, use main.py which will handle argument parsing.
    logger.info("update_google_sheet.py executed directly.")
    logger.info("This script is primarily designed to be called by main.py with arguments.")
    logger.info("Attempting run with placeholder/default values (may not work as intended without main.py):")
    
    # Example placeholder values - these would normally come from main.py via argparse
    example_product_data_path = "product_data.json"  # Default path if run standalone
    example_product_type = "testprod" 
    example_min_moq = None
    example_max_moq = None
    example_source_url = "https://example.com/default_product.html"  # Placeholder source URL
    example_google_sheet_id = "your_spreadsheet_id_here"  # Placeholder Google Sheet ID

    # Check if a default product_data.json exists, otherwise skip process_and_upload
    if os.path.exists(example_product_data_path):
        run_sheet_update(
            example_product_data_path, 
            example_product_type, 
            example_min_moq, 
            example_max_moq, 
            example_source_url, 
            example_google_sheet_id
        )
    else:
        logger.warning(f"Placeholder product_data.json ('{example_product_data_path}') not found.")
        logger.info("To test sheet header creation (if sheet service works), you might need to run parts manually or ensure a dummy file.")
        logger.info("Finished direct execution attempt of update_google_sheet.py.")
