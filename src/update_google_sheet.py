import os
import json
import datetime
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# --- Configuration ---
load_dotenv(override=True)

# Environment variables
GOOGLE_SHEET_ID_FROM_ENV = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_CREDENTIALS_PATH_FROM_ENV = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

print(f"DEBUG: GOOGLE_SHEET_ID from .env (raw): {GOOGLE_SHEET_ID_FROM_ENV}")
print(f"DEBUG: GOOGLE_APPLICATION_CREDENTIALS from .env (raw): {GOOGLE_CREDENTIALS_PATH_FROM_ENV}")

# Process GOOGLE_SHEET_ID
if GOOGLE_SHEET_ID_FROM_ENV and GOOGLE_SHEET_ID_FROM_ENV.startswith("https://docs.google.com/spreadsheets/d/"):
    try:
        GOOGLE_SHEET_ID = GOOGLE_SHEET_ID_FROM_ENV.split('/d/')[1].split('/')[0]
        print(f"DEBUG: Parsed GOOGLE_SHEET_ID: {GOOGLE_SHEET_ID}")
    except IndexError:
        print(f"ERROR: Could not parse GOOGLE_SHEET_ID from URL: {GOOGLE_SHEET_ID_FROM_ENV}")
        GOOGLE_SHEET_ID = None
else:
    GOOGLE_SHEET_ID = GOOGLE_SHEET_ID_FROM_ENV
    print(f"DEBUG: Using GOOGLE_SHEET_ID as is: {GOOGLE_SHEET_ID}")

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
EXPECTED_HEADER = ["id", "photo", "moq", "price 1688", "price cust", "profit", "info", "link"]
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# --- Google Sheets Helper Functions ---

def get_google_sheets_service():
    """Initializes and returns the Google Sheets API service client."""
    if not GOOGLE_CREDENTIALS_PATH:
        raise ValueError("GOOGLE_APPLICATION_CREDENTIALS environment variable not set or key file path is missing.")
    # Check for file existence *after* path resolution
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
        return None
    except HttpError as error:
        print(f"An API error occurred while fetching sheet metadata: {error}")
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
                            'startIndex': 0,  # Column A (0-indexed)
                            'endIndex': 1     # Column A
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
                            'startIndex': 1,  # Column B (0-indexed)
                            'endIndex': 2     # Column B
                        },
                        'properties': {'pixelSize': 200},
                        'fields': 'pixelSize'
                    }
                },
                {
                    'updateDimensionProperties': { # Set width for 'info' column (G)
                        'range': {
                            'sheetId': sheet_id,
                            'dimension': 'COLUMNS',
                            'startIndex': 6, # Column G
                            'endIndex': 7
                        },
                        'properties': {'pixelSize': 150},
                        'fields': 'pixelSize'
                    }
                },
                {
                    'updateDimensionProperties': { # Set width for 'link' column (H)
                        'range': {
                            'sheetId': sheet_id,
                            'dimension': 'COLUMNS',
                            'startIndex': 7, # Column H
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
        print(f"An API error occurred during header setup: {error}")
        raise

def process_and_upload_data(service, spreadsheet_id, sheet_name, product_data_path, product_type, min_moq, max_moq, sheet_id_val, source_url):
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
        parsed_price_ranges = []
        for tier in original_price_range_list:
            try:
                tier_copy = tier.copy() # Work on a copy
                tier_copy['startQuantity'] = int(tier_copy['startQuantity'])
                parsed_price_ranges.append(tier_copy)
            except (ValueError, TypeError, KeyError):
                print(f"DEBUG: Skipping tier due to invalid or missing 'startQuantity': {tier}")
                continue # Skip tiers that can't be parsed

        if not parsed_price_ranges:
            price_tiers_to_process.append({'moq': '', 'price1688': ''})
        else:
            list_after_min_filter = []
            if min_moq is not None:
                # Only include tiers that start at or after min_moq
                for tier in parsed_price_ranges:
                    if tier['startQuantity'] >= min_moq:
                        list_after_min_filter.append(tier)
            else: # No min_moq filter
                list_after_min_filter = parsed_price_ranges

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

        rows_to_append = []
        sku_processing_details = [] 
        
        date_prefix = datetime.datetime.now().strftime("%Y%m%d")
        
        # SKU ID formatting based on product_type argument
        if product_type:
            product_type_prefix_str = product_type.lower()
            product_id_template = f"{date_prefix}_{product_type_prefix_str}_{{counter:03d}}"
        else:
            product_id_template = f"{date_prefix}_{{counter:03d}}"
        
        sku_id_counter = start_sku_index

        for sku_info in product_sku_infos:
            item_id = product_id_template.format(counter=sku_id_counter)
            
            sku_image_url = ''
            if sku_info.get('skuAttributes') and isinstance(sku_info['skuAttributes'], list) and len(sku_info['skuAttributes']) > 0:
                for attr in sku_info['skuAttributes']:
                    if isinstance(attr, dict) and attr.get('skuImageUrl'):
                        sku_image_url = attr.get('skuImageUrl', '').strip()
                        break # Found the first image URL for this SKU
            
            image_formula = ''
            if sku_image_url: # Only create formula if URL exists
                # Make the image a hyperlink to itself
                image_formula = f'=HYPERLINK("{sku_image_url}", IMAGE("{sku_image_url}"))'
            else:
                image_formula = '' # Leave blank if no image URL

            num_rows_for_this_sku = 0
            for i, tier_data in enumerate(price_tiers_to_process):
                # ID, Photo, Info, OfferID only in the first row of the SKU group
                current_id = item_id if i == 0 else ""
                current_photo = image_formula if i == 0 else ""
                # For now, Info and OfferID are blank, but would follow the same pattern
                current_info = "" # Placeholder
                current_offerid = "" # Placeholder
                current_link = source_url if i == 0 else "" # Use the passed source_url for the link column
                
                row = [
                    current_id,
                    current_photo,
                    tier_data['moq'],
                    tier_data['price1688'],
                    "",  # price cust
                    "",  # profit
                    current_info if i == 0 else "",
                    current_link if i == 0 else ""
                ]
                rows_to_append.append(row)
                num_rows_for_this_sku += 1
            sku_processing_details.append(num_rows_for_this_sku)
            sku_id_counter += 1 # Increment for the next product_item

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

                    # --- Apply Cell Merges --- 
                    merge_requests = []
                    current_data_row_offset_for_merge = 0
                    
                    for num_rows_this_sku in sku_processing_details:
                        if num_rows_this_sku > 1:
                            merge_start_row_0_idx = actual_start_row_1_indexed - 1 + current_data_row_offset_for_merge
                            merge_end_row_0_idx_exclusive = merge_start_row_0_idx + num_rows_this_sku
                            
                            columns_to_merge_indices = [0, 1, 6, 7] # A, B, G, H
                            for col_idx in columns_to_merge_indices:
                                merge_requests.append({
                                    'mergeCells': {
                                        'range': {
                                            'sheetId': sheet_id_val, # Use sheet_id passed into the function
                                            'startRowIndex': merge_start_row_0_idx,
                                            'endRowIndex': merge_end_row_0_idx_exclusive,
                                            'startColumnIndex': col_idx,
                                            'endColumnIndex': col_idx + 1
                                        },
                                        'mergeType': 'MERGE_ALL' 
                                    }
                                })
                        current_data_row_offset_for_merge += num_rows_this_sku
                    
                    if merge_requests:
                        service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={'requests': merge_requests}).execute()
                        print(f"Successfully applied cell merges for multi-tier SKUs.")
                    # --- End Cell Merges ---

                    # --- Apply Row Heights (Dynamically per SKU block) ---
                    row_height_requests = []
                    current_data_row_offset_for_height = 0
                    SKU_BLOCK_TARGET_HEIGHT = 400 # Reverted to original target
                    # print(f"DEBUG: Using SKU_BLOCK_TARGET_HEIGHT = {SKU_BLOCK_TARGET_HEIGHT}px for testing.") # Keep original comment if needed

                    for i, num_rows_this_sku in enumerate(sku_processing_details):
                        if num_rows_this_sku > 0:
                            desired_individual_row_height = max(1, int(SKU_BLOCK_TARGET_HEIGHT / num_rows_this_sku))
                            # Apply workaround for observed doubling effect by the API/renderer
                            api_pixel_size_to_send = max(1, int(desired_individual_row_height / 2))
                            
                            abs_start_row_0_idx = (actual_start_row_1_indexed - 1) + current_data_row_offset_for_height
                            abs_end_row_0_idx_exclusive = abs_start_row_0_idx + num_rows_this_sku

                            print(f"DEBUG SKU Block {i+1}: num_rows={num_rows_this_sku}, desired_row_height={desired_individual_row_height}, api_sent_height={api_pixel_size_to_send}, range_0idx=[{abs_start_row_0_idx}-{abs_end_row_0_idx_exclusive-1}]")

                            row_height_requests.append({
                                'updateDimensionProperties': {
                                    'range': {
                                        'sheetId': sheet_id_val, # Use sheet_id passed into the function
                                        'dimension': 'ROWS',
                                        'startIndex': abs_start_row_0_idx,
                                        'endIndex': abs_end_row_0_idx_exclusive
                                    },
                                    'properties': {'pixelSize': api_pixel_size_to_send},
                                    'fields': 'pixelSize'
                                }
                            })
                        current_data_row_offset_for_height += num_rows_this_sku
                    
                    if row_height_requests:
                        service.spreadsheets().batchUpdate(
                            spreadsheetId=spreadsheet_id, body={'requests': row_height_requests}).execute()
                        print(f"Dynamically set row heights for SKU blocks. Target visual block height: {SKU_BLOCK_TARGET_HEIGHT}px.")
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

def run_sheet_update(product_data_path, product_type_arg, min_moq_arg, max_moq_arg, source_url_arg):
    """Main function to orchestrate the sheet update process."""
    print("Starting Google Sheet update process...")
    if not GOOGLE_SHEET_ID:
        print("Error: GOOGLE_SHEET_ID is not set. Please check your .env file or environment variables.")
        return

    service = None
    try:
        service = get_google_sheets_service()
    except ValueError as e:
        print(f"Error initializing Google Sheets service: {e}")
        return
    except Exception as e:
        print(f"An unexpected error occurred during service initialization: {e}")
        return

    if not service:
        print("Failed to initialize Google Sheets service. Exiting.")
        return

    target_sheet_id_val = None
    try:
        # Ensure header and basic column formatting first
        # Get the numeric sheet ID for the target sheet name
        target_sheet_id_val = get_sheet_id_by_name(service, GOOGLE_SHEET_ID, TARGET_SHEET_NAME)
        if target_sheet_id_val is None:
            print(f"Error: Could not find sheet named '{TARGET_SHEET_NAME}' in spreadsheet '{GOOGLE_SHEET_ID}'.")
            return
        
        ensure_header_and_freeze(service, GOOGLE_SHEET_ID, target_sheet_id_val, TARGET_SHEET_NAME, EXPECTED_HEADER)

        # Process and upload data from the specified JSON file
        process_and_upload_data(service, GOOGLE_SHEET_ID, TARGET_SHEET_NAME, 
                                product_data_path, product_type_arg, 
                                min_moq_arg, max_moq_arg, target_sheet_id_val, source_url_arg)

    except HttpError as e:
        print(f'An API error occurred in main: {e}')
        # Further error details can be printed here if needed
    except Exception as e:
        print(f'An unexpected error occurred in main: {e}')
        import traceback
        traceback.print_exc()

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

    # Check if a default product_data.json exists, otherwise skip process_and_upload
    if os.path.exists(example_product_data_path):
        run_sheet_update(example_product_data_path, example_product_type, example_min_moq, example_max_moq, example_source_url)
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
