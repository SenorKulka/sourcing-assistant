import os
import json
import datetime
from urllib.parse import urlparse, urlunparse
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# --- Configuration ---
load_dotenv(override=True)

# Environment variables
# GOOGLE_SHEET_ID_FROM_ENV = os.getenv("GOOGLE_SHEET_ID") # No longer primary source for run_sheet_update
GOOGLE_CREDENTIALS_PATH_FROM_ENV = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# print(f"DEBUG: GOOGLE_SHEET_ID from .env (raw): {{GOOGLE_SHEET_ID_FROM_ENV}}") # Commented out
print(f"DEBUG: GOOGLE_APPLICATION_CREDENTIALS from .env (raw): {GOOGLE_CREDENTIALS_PATH_FROM_ENV}")

# Global GOOGLE_SHEET_ID processing is removed as run_sheet_update will take it as a parameter.
# GOOGLE_SHEET_ID = None # Ensuring it's not used globally by mistake.

# Process GOOGLE_CREDENTIALS_PATH
# If the path from .env starts with './', make it absolute relative to the script's directory or CWD.
# For simplicity, we'll assume it's relative to the CWD where the script is run.
if GOOGLE_CREDENTIALS_PATH_FROM_ENV and GOOGLE_CREDENTIALS_PATH_FROM_ENV.startswith('./'):
    # Construct absolute path from CWD. Note: os.path.abspath handles './' correctly.
    GOOGLE_CREDENTIALS_PATH = os.path.abspath(GOOGLE_CREDENTIALS_PATH_FROM_ENV)
    print(f"DEBUG: Resolved GOOGLE_APPLICATION_CREDENTIALS (relative to CWD): {GOOGLE_CREDENTIALS_PATH}")
elif GOOGLE_CREDENTIALS_PATH_FROM_ENV:
    GOOGLE_CREDENTIALS_PATH = GOOGLE_CREDENTIALS_PATH_FROM_ENV
    print(f"DEBUG: Using GOOGLE_APPLICATION_CREDENTIALS as is (absolute or from global env): {GOOGLE_CREDENTIALS_PATH}")
else:
    GOOGLE_CREDENTIALS_PATH = None
    print("DEBUG: GOOGLE_APPLICATION_CREDENTIALS not found in .env or environment.")

# Script configurations
TARGET_SHEET_NAME = "Sheet1"  # The name of the sheet to update
EXPECTED_HEADER = ["id", "photo", "price 1688", "price cust", "profit", "moq", "info", "link"]
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

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

def process_and_upload_data(service, spreadsheet_id, sheet_name, product_data_path, product_type, min_moq, max_moq, sheet_id_val, source_url):
    # Clean the source_url to remove query parameters and fragments
    if source_url:
        parsed_url_obj = urlparse(source_url)
        source_url = urlunparse(parsed_url_obj._replace(query='', fragment=''))

    try:
        with open(product_data_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        product_sku_infos = data.get('result', {}).get('result', {}).get('productSkuInfos', [])
        if not product_sku_infos:
            print("No product SKU information found in the JSON data.")
            return

        # --- MOQ Filtering for priceRangeList --- 
        price_tiers_to_process = []
        original_price_range_list = data.get('result', {}).get('result', {}).get('productSaleInfo', {}).get('priceRangeList', []) 
        
        # Convert startQuantity to int for reliable comparison
        temp_parsed_price_ranges = [] # Use a temporary list first
        for tier in original_price_range_list:
            try:
                tier_copy = tier.copy() # Work on a copy
                tier_copy['startQuantity'] = int(tier_copy['startQuantity'])
                temp_parsed_price_ranges.append(tier_copy)
            except (ValueError, TypeError, KeyError):
                print(f"DEBUG: Skipping tier due to invalid or missing 'startQuantity': {tier}")
                continue # Skip tiers that can't be parsed
        
        # Sort by startQuantity to ensure correct logic for MOQ filtering
        # Use .get() for robustness in key access, defaulting to infinity if somehow startQuantity is missing
        parsed_price_ranges = sorted(temp_parsed_price_ranges, key=lambda t: t.get('startQuantity', float('inf')))

        if not parsed_price_ranges:
            price_tiers_to_process.append({'moq': '', 'price1688': ''})
        else:
            list_after_min_filter = []
            if min_moq is not None:
                s_active = None
                # Find the startQuantity of the tier whose range min_moq falls into
                for i, current_tier_data in enumerate(parsed_price_ranges):
                    current_tier_start_val = current_tier_data['startQuantity']
                    next_tier_start_val = parsed_price_ranges[i+1]['startQuantity'] if i + 1 < len(parsed_price_ranges) else float('inf')
                    
                    if current_tier_start_val <= min_moq < next_tier_start_val:
                        s_active = current_tier_start_val
                        break
                
                # If min_moq is less than the start_quantity of the very first tier,
                # it implies all tiers are effectively accessible. The 'active' tier is the first one.
                if s_active is None and parsed_price_ranges and min_moq < parsed_price_ranges[0]['startQuantity']:
                    s_active = parsed_price_ranges[0]['startQuantity'] 
                
                if s_active is not None:
                    for tier_data_to_filter in parsed_price_ranges:
                        if tier_data_to_filter['startQuantity'] >= s_active:
                            list_after_min_filter.append(tier_data_to_filter)
                # If s_active is None at this point (e.g., min_moq is very high, beyond all tiers, or parsed_price_ranges was initially empty),
                # list_after_min_filter will remain empty, which correctly signifies no tiers meet the criteria.
            else: # No min_moq filter
                list_after_min_filter = list(parsed_price_ranges) # Use a copy

            filtered_price_range_list_final = []
            if max_moq is not None:
                for tier in list_after_min_filter:
                    if tier['startQuantity'] <= max_moq:
                        filtered_price_range_list_final.append(tier)
            else: # No max_moq filter
                filtered_price_range_list_final = list_after_min_filter
            
            if filtered_price_range_list_final:
                for tier in filtered_price_range_list_final:
                    price_tiers_to_process.append({
                        'moq': str(tier.get('startQuantity', '')),
                        'price1688': str(tier.get('price', ''))
                    })
            else: # Filters resulted in an empty list
                price_tiers_to_process.append({'moq': '', 'price1688': ''})
        # --- End MOQ Filtering --- 

        # Determine the starting SKU index based on existing data in the sheet
        start_sku_index = 1
        try:
            range_to_get_ids = f"{sheet_name}!A1:A"
            id_column_data = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_to_get_ids).execute()
            all_id_values_in_col_A = id_column_data.get('values', [])
            last_id_str = None
            if all_id_values_in_col_A and len(all_id_values_in_col_A) > 1:
                for row_idx in range(len(all_id_values_in_col_A) - 1, 0, -1):
                    if all_id_values_in_col_A[row_idx] and all_id_values_in_col_A[row_idx][0]:
                        potential_id = str(all_id_values_in_col_A[row_idx][0]).strip()
                        if potential_id.lower() != EXPECTED_HEADER[0].lower(): 
                            last_id_str = potential_id
                            break 
            if last_id_str:
                parts = last_id_str.split('_')
                # Check if the last part is a number (sequence), and potentially a product type before it
                if len(parts) >= 2 and parts[-1].isdigit(): 
                    current_max_seq = int(parts[-1])
                    start_sku_index = current_max_seq + 1
                    print(f"DEBUG: Found last ID '{last_id_str}', next SKU sequence starts at: {start_sku_index}")
                else:
                    print(f"DEBUG: Last ID '{last_id_str}' format not recognized for sequencing (expected ..._NNN). Defaulting SKU sequence to 1.")
            else:
                print("DEBUG: No existing data SKUs found or sheet is empty/header-only. Starting SKU sequence from 1.")
        except HttpError as e:
            print(f"DEBUG: API error when trying to read existing IDs for sequencing: {e}. Defaulting SKU sequence to 1.")
        except Exception as e:
            print(f"DEBUG: Non-API error during ID sequence processing: {e}. Defaulting SKU sequence to 1.")

        # First, prepare all SKU information
        sku_data = []
        date_prefix = datetime.datetime.now().strftime("%Y%m%d")
        
        # SKU ID formatting based on product_type argument
        if product_type:
            product_type_prefix_str = product_type.lower()
            product_id_template = f"{date_prefix}_{product_type_prefix_str}_{{counter:03d}}"
        else:
            product_id_template = f"{date_prefix}_{{counter:03d}}"
        
        sku_id_counter = start_sku_index
        
        # First pass: collect all SKU data
        for sku_info in product_sku_infos:
            item_id = product_id_template.format(counter=sku_id_counter)
            
            # Get SKU image URL
            sku_image_url = ''
            if sku_info.get('skuAttributes') and isinstance(sku_info['skuAttributes'], list) and len(sku_info['skuAttributes']) > 0:
                for attr in sku_info['skuAttributes']:
                    if isinstance(attr, dict) and attr.get('skuImageUrl'):
                        sku_image_url = attr.get('skuImageUrl', '').strip()
                        break
            
            image_formula = ''
            if sku_image_url:
                image_formula = f'=HYPERLINK("{sku_image_url}", IMAGE("{sku_image_url}"))'
            
            # Get SKU direct price if available
            sku_direct_price = str(sku_info['price']) if sku_info.get('price') else None
            
            # Get SKU info text from attribute 3216
            sku_info_text = ""
            if sku_info.get('skuAttributes') and isinstance(sku_info['skuAttributes'], list):
                for attr in sku_info['skuAttributes']:
                    if isinstance(attr, dict) and attr.get('attributeId') == 3216 and 'valueTrans' in attr:
                        sku_info_text = str(attr['valueTrans'])
                        break
            
            sku_data.append({
                'id': item_id,
                'image': image_formula,
                'info': sku_info_text,
                'direct_price': sku_direct_price,
                'link': source_url
            })
            
            sku_id_counter += 1
        
        # Second pass: create rows grouped by price/moq
        rows_to_append = []
        price_moq_groups = []
        
        # Process each price/moq tier
        for tier_data in price_tiers_to_process:
            moq = tier_data['moq']
            price = tier_data['price1688']
            
            # Add all SKUs for this price/moq tier
            for sku in sku_data:
                # Use SKU direct price if available, otherwise use the tier price
                price_to_use = sku['direct_price'] if sku['direct_price'] else price
                
                row = [
                    sku['id'],  # Always include ID
                    sku['image'],  # Always include image
                    price_to_use,  # Price for this tier/SKU
                    "",  # price cust (empty)
                    "",  # profit (empty)
                    moq,  # MOQ for this tier (moved after profit)
                    sku['info'],  # Always include info
                    sku['link']  # Always include link
                ]
                rows_to_append.append(row)
            
            # Track the number of rows for this price/moq group for merging
            if sku_data:  # Only add if there are SKUs
                price_moq_groups.append({
                    'start_row': len(rows_to_append) - len(sku_data),
                    'end_row': len(rows_to_append),
                    'moq': moq
                })

        if not rows_to_append:
            print("No data processed to upload.")
            return

        # DEBUG: Print the first few rows to be appended
        print(f"DEBUG: rows_to_append (first 3 rows): {rows_to_append[:3]}")

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
        
        print(f"Successfully uploaded {len(rows_to_append)} product SKUs to '{sheet_name}'.")

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
                                        'startRowIndex': actual_start_row_1_indexed - 1 + group['start_row'],
                                        'endRowIndex': actual_start_row_1_indexed - 1 + group['end_row'],
                                        'startColumnIndex': 5,  # MOQ column (F)
                                        'endColumnIndex': 6     # End at column G (exclusive)
                                    },
                                    'mergeType': 'MERGE_ALL'
                                }
                            })
                    
                    if merge_requests:
                        service.spreadsheets().batchUpdate(
                            spreadsheetId=spreadsheet_id, 
                            body={'requests': merge_requests}
                        ).execute()
                        print(f"Successfully merged MOQ cells for {len(merge_requests)} price/moq groups.")
                    # --- End Cell Merges ---

                    # --- Set Row Height to 400px for each SKU ---
                    row_height_requests = []
                    TARGET_ROW_HEIGHT = 400  # 400px per SKU row
                    
                    # Apply height to all rows
                    row_height_requests.append({
                        'updateDimensionProperties': {
                            'range': {
                                'sheetId': sheet_id_val,
                                'dimension': 'ROWS',
                                'startIndex': actual_start_row_1_indexed - 1,
                                'endIndex': actual_start_row_1_indexed - 1 + len(rows_to_append)
                            },
                            'properties': {'pixelSize': TARGET_ROW_HEIGHT // 2},  # Halved due to API rendering quirk
                            'fields': 'pixelSize'
                        }
                    })
                    
                    if row_height_requests:
                        service.spreadsheets().batchUpdate(
                            spreadsheetId=spreadsheet_id, 
                            body={'requests': row_height_requests}
                        ).execute()
                        print(f"Set consistent row height of {TARGET_ROW_HEIGHT}px for all rows.")
                    # --- End Row Heights ---

                    # --- Apply Final Cell Formatting (Alignment, Currency, Formulas) ---
                    formatting_requests = []
                    actual_end_row_1_indexed = (actual_start_row_1_indexed -1) + len(rows_to_append) # Calculate end row based on how many rows were prepared

                    # 0. Ensure data rows are NOT bold (overrides any prior formatting)
                    formatting_requests.append({
                        'repeatCell': {
                            'range': {
                                'sheetId': sheet_id_val,
                                'startRowIndex': actual_start_row_1_indexed - 1, 
                                'endRowIndex': actual_end_row_1_indexed,
                                'startColumnIndex': 0, # Column A
                                'endColumnIndex': 8  # Column H
                            },
                            'cell': {
                                'userEnteredFormat': {
                                    'textFormat': {'bold': False}
                                }
                            },
                            'fields': 'userEnteredFormat.textFormat.bold'
                        }
                    })

                    # 1. Center all added cells (A to H for the new rows)
                    formatting_requests.append({
                        'repeatCell': {
                            'range': {
                                'sheetId': sheet_id_val,
                                'startRowIndex': actual_start_row_1_indexed - 1, # 0-indexed
                                'endRowIndex': actual_end_row_1_indexed, # Exclusive
                                'startColumnIndex': 0, # Column A
                                'endColumnIndex': 8  # Column H is index 7, so endIndex is 8
                            },
                            'cell': {
                                'userEnteredFormat': {
                                    'horizontalAlignment': 'CENTER',
                                    'verticalAlignment': 'MIDDLE'
                                }
                            },
                            'fields': 'userEnteredFormat(horizontalAlignment,verticalAlignment)'
                        }
                    })

                    # 2. Format 'price 1688' (Column D, index 3) as CNY
                    formatting_requests.append({
                        'repeatCell': {
                            'range': {
                                'sheetId': sheet_id_val,
                                'startRowIndex': actual_start_row_1_indexed - 1,
                                'endRowIndex': actual_end_row_1_indexed,
                                'startColumnIndex': 3, # Column D
                                'endColumnIndex': 4
                            },
                            'cell': {
                                'userEnteredFormat': {
                                    'numberFormat': {'type': 'CURRENCY', 'pattern': '¥#,##0.00'}
                                }
                            },
                            'fields': 'userEnteredFormat.numberFormat'
                        }
                    })

                    # 3. Format 'price cust' (Column E, index 4) as CNY
                    formatting_requests.append({
                        'repeatCell': {
                            'range': {
                                'sheetId': sheet_id_val,
                                'startRowIndex': actual_start_row_1_indexed - 1,
                                'endRowIndex': actual_end_row_1_indexed,
                                'startColumnIndex': 4, # Column E
                                'endColumnIndex': 5
                            },
                            'cell': {
                                'userEnteredFormat': {
                                    'numberFormat': {'type': 'CURRENCY', 'pattern': '¥#,##0.00'}
                                }
                            },
                            'fields': 'userEnteredFormat.numberFormat'
                        }
                    })

                    # 4. Set Profit Formulas (Column F, index 5)
                    for i in range(len(rows_to_append)):
                        current_sheet_row = actual_start_row_1_indexed + i
                        # Profit formula: =IF(AND(ISNUMBER(D<row>), ISNUMBER(E<row>)), E<row>-D<row>, "")
                        formula = f'=IF(AND(ISNUMBER(D{current_sheet_row}), ISNUMBER(E{current_sheet_row})), E{current_sheet_row}-D{current_sheet_row}, "")'
                        formatting_requests.append({
                            'updateCells': {
                                'rows': [{
                                    'values': [{
                                        'userEnteredValue': {'formulaValue': formula}
                                    }]
                                }],
                                'fields': 'userEnteredValue',
                                'range': {
                                    'sheetId': sheet_id_val,
                                    'startRowIndex': current_sheet_row - 1, # 0-indexed
                                    'endRowIndex': current_sheet_row,
                                    'startColumnIndex': 5, # Column F
                                    'endColumnIndex': 6
                                }
                            }
                        })
                    
                    # 5. Set Text Wrapping for 'info' column (G, index 6)
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
        print(f"An API error occurred during data upload: {error}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred during data processing: {e}")
        raise

def run_sheet_update(product_data_path, product_type_arg, min_moq_arg, max_moq_arg, source_url_arg, google_sheet_id_param):
    """Main function to orchestrate the sheet update process using a provided Google Sheet ID."""
    
    if not google_sheet_id_param:
        error_msg = "Google Sheet ID is required but was not provided to run_sheet_update."
        print(f"ERROR: {error_msg}")
        raise ValueError(error_msg)

    print(f"--- Starting Google Sheet Update for Sheet ID: {google_sheet_id_param} ---")
    print(f"Product Type: {product_type_arg}, Min MOQ: {min_moq_arg}, Max MOQ: {max_moq_arg}")
    print(f"Source URL: {source_url_arg}, Data Path: {product_data_path}")

    try:
        service = get_google_sheets_service()
        print("Google Sheets service initialized successfully.")

        sheet_id_val = get_sheet_id_by_name(service, google_sheet_id_param, TARGET_SHEET_NAME)

        if sheet_id_val is None:
            if TARGET_SHEET_NAME:
                print(f"Sheet named '{TARGET_SHEET_NAME}' not found in spreadsheet '{google_sheet_id_param}'. Attempting to create it.")
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
                    response = service.spreadsheets().batchUpdate(spreadsheetId=google_sheet_id_param, body=add_sheet_request_body).execute()
                    new_sheet_properties = response.get('replies')[0].get('addSheet').get('properties')
                    sheet_id_val = new_sheet_properties.get('sheetId')
                    print(f"Successfully created sheet '{TARGET_SHEET_NAME}' with ID: {sheet_id_val}")
                except HttpError as error_create:
                    err_msg = f"Failed to create sheet '{TARGET_SHEET_NAME}' in spreadsheet '{google_sheet_id_param}': {error_create}"
                    print(f"ERROR: {err_msg}")
                    raise ValueError(err_msg)
            else:
                 err_msg = f"Target sheet name ('{TARGET_SHEET_NAME}') not found in spreadsheet '{google_sheet_id_param}' and no sheet name configured to create."
                 print(f"ERROR: {err_msg}")
                 raise ValueError(err_msg)

        ensure_header_and_freeze(service, google_sheet_id_param, sheet_id_val, TARGET_SHEET_NAME, EXPECTED_HEADER)
        print(f"Header check/update for sheet '{TARGET_SHEET_NAME}' (ID: {sheet_id_val}) complete.")

        process_and_upload_data(service, google_sheet_id_param, TARGET_SHEET_NAME, product_data_path, product_type_arg, min_moq_arg, max_moq_arg, sheet_id_val, source_url_arg)
        print(f"Data processing and upload for sheet '{TARGET_SHEET_NAME}' complete.")
        print(f"--- Google Sheet Update for Sheet ID: {google_sheet_id_param} Finished Successfully ---")

    except FileNotFoundError as e_fnf:
        print(f"ERROR during sheet update (FileNotFound) for '{google_sheet_id_param}': {e_fnf}")
        raise
    except ValueError as e_val:
        print(f"ERROR during sheet update (ValueError) for '{google_sheet_id_param}': {e_val}")
        raise
    except HttpError as e_http:
        print(f"API ERROR during sheet update for '{google_sheet_id_param}': {e_http}")
        print(f"Details: {e_http.content}")
        raise
    except Exception as e_general:
        print(f"UNEXPECTED ERROR during sheet update for '{google_sheet_id_param}': {e_general}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == '__main__':
    # This part is now more for structure; direct execution might require specific args.
    # For full functionality, use main.py which will handle argument parsing.
    print("update_google_sheet.py executed directly.")
    print("This script is primarily designed to be called by main.py with arguments.")
    print("Attempting run with placeholder/default values (may not work as intended without main.py):")
    
    # Example placeholder values - these would normally come from main.py via argparse
    example_product_data_path = "product_data.json" # Default path if run standalone
    example_product_type = "testprod" 
    example_min_moq = None
    example_max_moq = None
    example_source_url = "https://example.com/default_product.html" # Placeholder source URL
    example_google_sheet_id = "your_spreadsheet_id_here" # Placeholder Google Sheet ID

    # Check if a default product_data.json exists, otherwise skip process_and_upload
    if os.path.exists(example_product_data_path):
        run_sheet_update(example_product_data_path, example_product_type, example_min_moq, example_max_moq, example_source_url, example_google_sheet_id)
    else:
        print(f"Placeholder product_data.json ('{example_product_data_path}') not found.")
        print("To test sheet header creation (if sheet service works), you might need to run parts manually or ensure a dummy file.")
        # Optionally, could call just the header part if GOOGLE_SHEET_ID is set:
        # if GOOGLE_SHEET_ID:
        #     try:
        #         service = get_google_sheets_service()
        #         target_sheet_id_val = get_sheet_id(service, GOOGLE_SHEET_ID, TARGET_SHEET_NAME)
        #         if target_sheet_id_val is not None:
        #             ensure_header_and_freeze(service, GOOGLE_SHEET_ID, TARGET_SHEET_NAME, target_sheet_id_val)
        #         else:
        #             print(f"Sheet '{TARGET_SHEET_NAME}' not found for header test.")
        #     except Exception as e:
        #         print(f"Error during standalone header test: {e}")
        # else:
        #     print("GOOGLE_SHEET_ID not set, cannot test header creation.")

    print("Finished direct execution attempt of update_google_sheet.py.")
