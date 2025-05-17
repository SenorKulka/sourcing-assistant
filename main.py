import os
import argparse
import json
import tempfile
from dotenv import load_dotenv
from src.lovbuy_client import LovbuyClient
from src.update_google_sheet import run_sheet_update

load_dotenv(override=True)

LOVBUY_API_KEY = os.getenv("LOVBUY_API_KEY")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

def main():
    parser = argparse.ArgumentParser(description="Sourcing Assistant: Fetches 1688.com product data via LovBuy API and updates Google Sheets.")
    parser.add_argument("url", type=str, metavar="URL", help="The 1688.com product URL to fetch information for.")
    parser.add_argument("--product", type=str, required=False, help="Product identifier or name (e.g., 'T-Shirt A') used for sheet organization.")
    parser.add_argument("--minmoq", type=int, default=130, help="Minimum MOQ to consider for pricing tiers. Filters tiers shown in the sheet.")
    parser.add_argument("--maxmoq", type=int, default=None, help="Maximum MOQ to consider for pricing tiers. Filters tiers shown in the sheet.")

    args = parser.parse_args()

    print(f"Sourcing Assistant: Processing URL: {args.url} for product: {args.product}")

    if not LOVBUY_API_KEY:
        print("Error: LOVBUY_API_KEY not found in environment variables (.env file).")
        print("Please ensure your .env file is correctly set up with your LovBuy API key.")
        return

    if not GOOGLE_SHEET_ID:
        print("Error: GOOGLE_SHEET_ID not found in environment variables (.env file).")
        print("Please ensure your .env file is correctly set up with your Google Sheet ID or URL.")
        return

    if not GOOGLE_APPLICATION_CREDENTIALS:
        print("Error: GOOGLE_APPLICATION_CREDENTIALS not found in environment variables (.env file).")
        print("Please ensure your .env file is correctly set up with the path to your Google credentials JSON file.")
        return

    lovbuy = LovbuyClient(LOVBUY_API_KEY)

    try:
        product_info = lovbuy.get_product_info_from_1688_url(args.url)

        if product_info:
            print(f"\nSuccessfully fetched product data from LovBuy for URL: {args.url}")
            # print("--- Full API Response Body (JSON) ---")
            # print(json.dumps(product_info, indent=2, ensure_ascii=False))
            # print("-------------------------------------")

            # Create a temporary file to store the JSON data
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json', encoding='utf-8') as tmp_file:
                json.dump(product_info, tmp_file, ensure_ascii=False, indent=2)
                temp_product_data_path = tmp_file.name
            
            print(f"Product data temporarily saved to: {temp_product_data_path}")
            print(f"Attempting to update Google Sheet... Product: {args.product}, MinMOQ: {args.minmoq}, MaxMOQ: {args.maxmoq}")

            try:
                run_sheet_update(
                    product_data_path=temp_product_data_path,
                    product_type_arg=args.product,
                    min_moq_arg=args.minmoq,
                    max_moq_arg=args.maxmoq,
                    source_url_arg=args.url
                )
                print("Google Sheet update process completed.")
            except Exception as e_sheet:
                print(f"An error occurred during Google Sheet update: {e_sheet}")
            finally:
                # Clean up the temporary file
                if os.path.exists(temp_product_data_path):
                    os.remove(temp_product_data_path)
                    print(f"Temporary file {temp_product_data_path} removed.")

        else:
            print(f"Failed to retrieve product information from LovBuy for URL: {args.url}")
    except Exception as e_lovbuy:
        print(f"An error occurred while fetching product data from LovBuy: {e_lovbuy}")

if __name__ == "__main__":
    main()
