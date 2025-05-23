# Sourcing Assistant for 1688.com

This script automates fetching product information from 1688.com product URLs via the LovBuy API and uploads the structured data to a specified Google Sheet. It helps streamline the initial sourcing process by organizing product SKUs, prices, and MOQs directly into a spreadsheet.

## Features

* Fetches detailed product information from 1688.com (using LovBuy API).
* Extracts product variants (SKUs), including their images, MOQs, and tiered pricing.
* Uploads data to Google Sheets, creating a new entry for each product sourcing session.
* Generates unique IDs for each SKU, prefixed with the date and an optional product name.
* Formats the Google Sheet with a frozen header, bolded text, and specific column widths for better readability.
* Calculates a placeholder profit column in the sheet.
* Allows filtering of price tiers based on minimum (minmoq) and maximum (maxmoq) order quantities.
* **Enhanced User Feedback**: Provides detailed processing statistics including product names, SKU counts found, filtered, and uploaded to help track sourcing progress.

## How It Works

1. The script takes a 1688.com product URL as input.
2. It calls the LovBuy API to retrieve the product data in JSON format.
3. The script processes the JSON data, extracting relevant details like product title, SKU variations, images, and price tiers.
4. It connects to the specified Google Sheet using the Google Sheets API.
5. The script ensures a header row is present and formatted, then appends the processed product data into new rows.
6. Unique IDs are generated for each SKU to help track them.

## Data Handling Behavior

The script follows a specific hierarchy when processing and displaying product data.

### 1. Product vs SKU Data

* **When SKUs are available:**
  * Each SKU is listed as a separate row in the spreadsheet
  * SKU-specific data takes precedence over product-level data
  * If a SKU is missing certain attributes, they're filled from product-level data when available

* **When no SKUs are found:**
  * A single row is created using product-level data
  * All available product attributes are used

### 2. Data Priority (Highest to Lowest)

1. **SKU-specific attributes** (if available)
2. **Product-level attributes** (as fallback)
3. **Empty values** (if no data is available at any level)

### 3. Image Handling

* **Primary source:** SKU-specific image (from `skuAttributes.skuImageUrl`)
* **Fallback:** Main product image (first image from `result.result.productImage.images[0]`)
* **Format:** Images are embedded as clickable thumbnails with links to full-size images

### 4. Attribute Processing

For each SKU, the script processes these attributes in order:

1. **SKU ID**: Auto-generated with format `YYYYMMDD_PRODUCTNAME_###`
2. **Image**: As per image handling above
3. **Price**: SKU price → Product price → Empty if none
4. **Info Text**: SKU attribute 3216 → Product attribute 3216 → Empty
5. **Material**: SKU attribute 287 → Product attribute 287 → Empty
6. **Link**: Cleaned source URL (without query parameters)

### 5. Error Handling

* Missing SKU data falls back to product data
* Missing images are left empty
* All API errors are logged with detailed messages
* The script continues processing even if some data is missing

## User Experience

The application provides clear, real-time feedback during processing:

* **Processing Status**: Shows which product link is currently being processed
* **Detailed Results**: Displays product names (truncated for readability) and processing statistics
* **Success Messages**: Shows exactly how many SKUs were found, filtered by MOQ criteria, and uploaded
* **Error Handling**: Clear error messages with specific details when issues occur
* **Visual Indicators**: Color-coded messages with emoji indicators for easy status recognition

Example success message: *"✅ Successfully processed 'Wireless Bluetooth Headphones Pro...' - 15 SKUs found → 8 after filtering → 8 uploaded to Google Sheet"*

## Prerequisites

Before you begin, ensure you have the following installed:

1. **Python:** Version 3.9 or higher is recommended.
    * **Windows:** Download from [python.org](https://www.python.org/downloads/windows/). Make sure to check "Add Python to PATH" during installation.
    * **macOS:** Python usually comes pre-installed. You can also install it via [Homebrew](https://brew.sh/) (`brew install python`) or download from [python.org](https://www.python.org/downloads/macos/).
    * **Linux:** Python is typically pre-installed. You can install it using your distribution's package manager (e.g., `sudo apt update && sudo apt install python3 python3-pip python3-venv`).

2. **uv (Python Packaging Tool):** `uv` is a fast Python package installer and resolver, written in Rust.
    * **Installation (Windows):**
    To open PowerShell: Press Win + X and select "Windows PowerShell" or "Terminal", or search for "PowerShell" in the Start Menu.

    ```powershell
    irm https://astral.sh/uv/install.ps1 | iex
    ```

    * **Installation (macOS, Linux):**

    ```bash
    curl -LsSf https://astral.sh/uv/install.sh | sh
    ```

    * For other installation methods, refer to the [official `uv` documentation](https://github.com/astral-sh/uv#installation).

## Setup Instructions

1. **Clone the Repository:**

    ```bash
    git clone https://github.com/SenorKulka/sourcing-assistant.git
    cd sourcing-assistant
    ```

2. **Create and Activate Virtual Environment (using `uv`):**
    It's highly recommended to use a virtual environment to manage project dependencies.

    ```bash
    uv venv # Create a virtual environment (usually creates a .venv folder)
    # On Windows (PowerShell):
    .\.venv\Scripts\Activate.ps1

    # On macOS/Linux
    source .venv/bin/activate
    ```

3. **Install Dependencies:**
    The project uses `pyproject.toml` to manage dependencies. `uv` can install them directly.

    ```bash
    uv sync
    ```

4. **Set Up Environment Variables:**
    Create a `.env` file in the project root directory by copying the example:

    ```bash
    cp .env.example .env
    ```

    Now, edit the `.env` file with your actual credentials:

    * `LOVBUY_API_KEY`: Your API key for the LovBuy service.
        * You'll need to register at [LovBuy API](https://www.lovbuy.com/api.html) to get this.

    * `GOOGLE_SHEET_ID`: The ID of the Google Sheet where data will be uploaded.
        * You can get this from the URL of your Google Sheet: `https://docs.google.com/spreadsheets/d/YOUR_GOOGLE_SHEET_ID/edit`
        * **Note:** When using the web interface, the Google Sheet ID or Link entered on the page will be used, taking precedence over this `.env` value.

    * `GOOGLE_APPLICATION_CREDENTIALS`: The path to your Google Cloud service account JSON key file.
        * **How to get this:**
            1. Go to the [Google Cloud Console](https://console.cloud.google.com/).
            2. Create a new project or select an existing one.
            3. Enable the **Google Sheets API** for your project (Search for "Google Sheets API" in the API Library).
            4. Go to "Credentials" under "APIs & Services".
            5. Click "Create Credentials" -> "Service account".
            6. Fill in the service account details, grant it appropriate roles (e.g., "Editor" for Sheets access, or a more restrictive custom role).
            7. After creating the service account, click on it, go to the "Keys" tab.
            8. Click "Add Key" -> "Create new key" -> Select "JSON" and click "Create".
            9. A JSON file will be downloaded. Save this file in your project directory (e.g., in the root or a dedicated `config` folder). **Make sure this file is listed in your `.gitignore` file to prevent committing it to version control.**
            10. Set the `GOOGLE_APPLICATION_CREDENTIALS` in your `.env` file to the path of this JSON file (e.g., `GOOGLE_APPLICATION_CREDENTIALS=./your-service-account-key.json`).

## Running the Application

Once everything is set up, you can run the application. Ensure your virtual environment is activated.

### Using the Web Interface (Recommended)

1. **Start the Flask Server:**

    ```bash
    uv run python main.py
    ```

    This will start the web application, typically on `http://127.0.0.1:5000`.

2. **Access in Browser:**
    Open your web browser and go to `http://127.0.0.1:5000`.

3. **Fill in the Form:**
    You will see a form with the following fields:
    * **Product Link:** (Required) The full URL of the 1688.com product page.
    * **Product Name:** (Optional) A name for the product, used in generating SKU IDs.
    * **Min MOQ:** (Optional) Minimum order quantity. Price tiers below this will be excluded. If left empty, defaults to `120`.
    * **Max MOQ:** (Optional) Maximum order quantity. Price tiers above this will be excluded.
    * **Google Sheet Link/ID:** (Required) The full URL of your Google Sheet (e.g., `https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit`) or just the Google Sheet ID itself.

4. **Submit:**
    Click the "Submit" button. The backend will process the request, fetch data from LovBuy, and update your Google Sheet.

### As a Command-Line Tool (Alternative)

You can also run the script directly from your terminal with arguments.

**Command Structure:**

```bash
uv run python main.py <1688_PRODUCT_URL> --product <PRODUCT_NAME> [--minmoq <MIN_MOQ>] [--maxmoq <MAX_MOQ>]
```

**Arguments:**

* `<1688_PRODUCT_URL>`: (Positional, Required) The full URL of the 1688.com product page you want to source.
* `--product <PRODUCT_NAME>`: (Optional, but recommended) A name or identifier for the product (e.g., "Red T-Shirt Model A", "ZokiSweater"). This is used in generating unique IDs in the Google Sheet. If not provided, IDs might be less descriptive.
* `--minmoq <MIN_MOQ>`: (Optional, default: 120) The minimum order quantity. Price tiers starting below this MOQ will be excluded from the sheet. Example: `--minmoq 100`.
* `--maxmoq <MAX_MOQ>`: (Optional, default: None) The maximum order quantity. Price tiers starting above this MOQ will be excluded. Example: `--maxmoq 500`.

**Example Usage:**

```bash
# Basic usage with a product name
uv run python main.py https://detail.1688.com/offer/xxxxxxxxxxxx.html --product "CoolWidget"

# With MOQ filtering
uv run python main.py https://detail.1688.com/offer/yyyyyyyyyyyy.html --product "GadgetPro" --minmoq 50 --maxmoq 1000
```

## Platform-Specific Notes

* **File Paths (Windows):** When setting `GOOGLE_APPLICATION_CREDENTIALS` in `.env` on Windows, use forward slashes (e.g., `C:/Users/YourUser/Projects/sourcing-assistant/keyfile.json`) or double backslashes (e.g., `C:\Users\YourUser\Projects\sourcing-assistant\keyfile.json`). Relative paths like `./keyfile.json` should work fine if the key is in the project root.
* **Shell Commands:** The virtual environment activation commands differ slightly between shells (bash/zsh vs. PowerShell vs. CMD). Refer to the setup section.

## Troubleshooting

* **`LOVBUY_API_KEY not found` / `GOOGLE_SHEET_ID not found` / `GOOGLE_APPLICATION_CREDENTIALS not found`:** Ensure your `.env` file is correctly named, located in the project root, and contains the correct keys and values.
* **`FileNotFoundError: [Errno 2] No such file or directory: 'your-service-account-key.json'`:** Double-check the path specified in `GOOGLE_APPLICATION_CREDENTIALS` in your `.env` file. Ensure it correctly points to your downloaded JSON key file. The path can be absolute or relative to the project root.
* **Google Sheets API Errors (HttpError 403, etc.):**
  * Ensure the Google Sheets API is enabled in your Google Cloud Project.
  * Verify the service account has permissions to edit the target Google Sheet. You might need to share the Google Sheet with the service account's email address (found in its details in the Google Cloud Console) giving it "Editor" access.
* **`uv: command not found`:** Ensure `uv` was installed correctly and its installation directory is in your system's PATH.
* **Dependency Issues:** If `uv sync` or `uv pip install` fails, check your internet connection and ensure `pyproject.toml` (or `requirements.txt`) is correctly formatted.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
