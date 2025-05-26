import os
import requests
import re
import json

class LovbuyClient:
    BASE_URL = "https://www.lovbuy.com/"
    API_V2_1688_ENDPOINT = "1688api/getproductinfo2.php"

    def __init__(self, api_key):
        self.api_key = api_key
        if not self.api_key:
            print("ERROR: LOVBUY_API_KEY not found in environment variables.")
            raise ValueError("API key is required.")
        
        print(f"LovbuyClient initialized for 1688 API v2.")
        print(f"Using API Key: {self.api_key[:4]}...{self.api_key[-4:]}")
        print(f"Target API URL: {self.BASE_URL}{self.API_V2_1688_ENDPOINT}")
        print("Parameters: 'key' (API Key), 'item_id' (extracted from URL), 'lang' (optional, default 'en').")
        print("Refer to: https://www.lovbuy.com/api5.html for '1688 API /Get a product info V.2'")

    def _request(self, method, endpoint, params=None, data=None, headers=None):
        url = f"{self.BASE_URL}{endpoint}"
        print(f"\nAttempting {method} request to: {url}")
        if params:
            print(f"With parameters: {params}")
        
        try:
            response = requests.request(method, url, params=params, json=data, headers=headers)
            print(f"Response Status Code: {response.status_code}")
            
            # Try to parse JSON response regardless of status code
            try:
                json_response = response.json()
                print(f"Response JSON: {json_response}")
                
                # If status code indicates success, return the response
                if response.status_code == 200:
                    return json_response
                
                # If status code indicates error, but we have JSON response, 
                # return it so the error details can be processed
                return json_response
                
            except ValueError as json_err:
                print(f"JSON decode error: {json_err}")
                print(f"Response content: {response.text}")
                # If we can't parse JSON, raise the HTTP error
                response.raise_for_status()
                
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            print(f"Response content: {response.text}") 
        except requests.exceptions.RequestException as req_err:
            print(f"Request exception occurred: {req_err}")
            
        return None

    def _extract_item_id_from_url(self, product_url):
        match = re.search(r"offer/(\d+)\.html", product_url)
        if match:
            return match.group(1)
        match = re.search(r"/(\d+)\.html", product_url) 
        if match:
            return match.group(1)
        print(f"Could not extract item_id from URL: {product_url}")
        return None

    def get_product_info_from_1688_url(self, product_url, lang="en"):
        item_id = self._extract_item_id_from_url(product_url)
        if not item_id:
            return None

        params = {
            "key": self.api_key,
            "item_id": item_id,
            "lang": lang
        }
        return self._request("GET", self.API_V2_1688_ENDPOINT, params=params)

if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    load_dotenv()
    test_api_key = os.getenv("LOVBUY_API_KEY")
    
    if not test_api_key:
        print("Error: LOVBUY_API_KEY not found in .env file for direct testing.")
    else:
        client = LovbuyClient(api_key=test_api_key)
        test_product_1688_url = "https://detail.1688.com/offer/856749453424.html" 
        print(f"\n--- Testing get_product_info_from_1688_url with: {test_product_1688_url} ---")
        product_data = client.get_product_info_from_1688_url(test_product_1688_url)
        if product_data:
            print("--- Full API Response Body (JSON) ---")
            print(json.dumps(product_data, indent=2, ensure_ascii=False))
            print("-------------------------------------")
            print(f"\nSuccessfully retrieved data for {test_product_1688_url}.")
        else:
            print(f"\nFailed to retrieve data for {test_product_1688_url}.")
    print("\nLovbuyClient 1688 API v2 test executed.")
