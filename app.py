from flask import Flask, request, jsonify
import requests
import pandas as pd
import os
from functools import lru_cache
import logging
import io

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
API_KEY = os.environ.get("API_KEY", "25c787e461c18a4a2a502ce49423a2808a68da65")
CAMPAIGN_ID = "323747"
OFFERS = {"tier_1": "11558", "tier_2": "22222", "tier_3": "33333"}

GITHUB_BASE_URL = "https://raw.githubusercontent.com/HannielSolutions/zip-routing-api/main"
# Since all data is in one file, we can use the same file for all tiers
SHEET_FILES = {
    "tier_1": f"{GITHUB_BASE_URL}/Tier%201.xlsx",
    "tier_2": f"{GITHUB_BASE_URL}/Tier%201.xlsx",  # Same file, different filter
    "tier_3": f"{GITHUB_BASE_URL}/Tier%201.xlsx"   # Same file, different filter
}

def download_and_process_excel(url, tier):
    """Download and process Excel file - handle the actual structure with PriceTier column."""
    try:
        logger.info(f"Downloading {tier} from {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        logger.info(f"Downloaded {len(response.content)} bytes for {tier}")
        
        # Read the Excel file
        content = io.BytesIO(response.content)
        df = pd.read_excel(content)
        logger.info(f"Excel shape: {df.shape}")
        logger.info(f"Columns: {df.columns.tolist()}")
        
        if df.empty:
            return set()
        
        # The file has "Zip Code" and "PriceTier" columns
        # We need to filter by the tier we want
        tier_mapping = {
            "tier_1": "Tier 1",
            "tier_2": "Tier 2", 
            "tier_3": "Tier 3"
        }
        
        target_tier = tier_mapping.get(tier)
        if not target_tier:
            logger.error(f"Unknown tier: {tier}")
            return set()
        
        # Filter rows where PriceTier matches our target
        if 'PriceTier' in df.columns and 'Zip Code' in df.columns:
            filtered_df = df[df['PriceTier'] == target_tier]
            logger.info(f"Found {len(filtered_df)} rows for {target_tier}")
            
            # Get ZIP codes and convert to 5-digit strings
            zip_codes = []
            for zip_val in filtered_df['Zip Code']:
                if pd.notna(zip_val):
                    zip_str = str(int(zip_val)).zfill(5)  # Convert to int first to remove decimals, then pad
                    zip_codes.append(zip_str)
            
            logger.info(f"Extracted ZIP codes for {tier}: {zip_codes[:20]}")
            logger.info(f"Total ZIP codes for {tier}: {len(zip_codes)}")
            logger.info(f"Contains 07004: {'07004' in zip_codes}")
            
            return set(zip_codes)
        else:
            logger.error(f"Expected columns not found. Available columns: {df.columns.tolist()}")
            return set()
            
    except Exception as e:
        logger.error(f"Failed to process {tier}: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return set()

@lru_cache(maxsize=1)
def load_zip_sets():
    """Load ZIP codes from all Excel files."""
    zip_sets = {}
    for tier, url in SHEET_FILES.items():
        zip_sets[tier] = download_and_process_excel(url, tier)
        logger.info(f"Loaded {len(zip_sets[tier])} ZIPs for {tier}")
    return zip_sets

# Raw inspection endpoint
@app.route("/inspect-file/<tier>", methods=["GET"])
def inspect_file(tier):
    if tier not in SHEET_FILES:
        return jsonify({"error": "Invalid tier"}), 400
    
    url = SHEET_FILES[tier]
    try:
        # Download raw file
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        content = response.content
        
        # Try to read with pandas in multiple ways
        results = {}
        
        # Method 1: Read everything, no assumptions
        try:
            df1 = pd.read_excel(io.BytesIO(content))
            results["method1_default"] = {
                "shape": df1.shape,
                "columns": df1.columns.tolist(),
                "first_10_rows": df1.head(10).to_dict('records')
            }
        except Exception as e:
            results["method1_default"] = {"error": str(e)}
        
        # Method 2: Read with no header
        try:
            df2 = pd.read_excel(io.BytesIO(content), header=None)
            results["method2_no_header"] = {
                "shape": df2.shape,
                "first_column_first_20": df2.iloc[:20, 0].tolist() if not df2.empty else []
            }
        except Exception as e:
            results["method2_no_header"] = {"error": str(e)}
        
        # Method 3: Check if it's actually an Excel file
        try:
            # Try to identify file format
            is_excel = content.startswith(b'PK') or content.startswith(b'\xd0\xcf\x11\xe0')
            results["file_analysis"] = {
                "size": len(content),
                "first_50_bytes": content[:50].hex(),
                "appears_to_be_excel": is_excel,
                "starts_with_pk": content.startswith(b'PK'),  # ZIP/XLSX format
                "starts_with_d0cf": content.startswith(b'\xd0\xcf\x11\xe0')  # Old Excel format
            }
        except Exception as e:
            results["file_analysis"] = {"error": str(e)}
        
        return jsonify({
            "tier": tier,
            "url": url,
            "analysis": results
        })
        
    except Exception as e:
        return jsonify({"error": str(e), "tier": tier, "url": url})

# Test endpoint
@app.route("/test-download/<tier>", methods=["GET"])
def test_download(tier):
    if tier not in SHEET_FILES:
        return jsonify({"error": "Invalid tier"}), 400
    
    zip_set = download_and_process_excel(SHEET_FILES[tier], tier)
    
    return jsonify({
        "tier": tier,
        "zip_count": len(zip_set),
        "sample_zips": sorted(list(zip_set))[:30],  # Show first 30 sorted
        "contains_07004": "07004" in zip_set,
        "contains_01451": "01451" in zip_set,  # First ZIP from your data
        "all_zips": sorted(list(zip_set)) if len(zip_set) < 50 else "too_many_to_show"
    })

# Test caller_id format endpoint
@app.route("/test-caller-id", methods=["POST"])
def test_caller_id():
    """Test different caller_id formats to see what works."""
    data = request.get_json()
    caller_id = data.get("caller_id")
    
    if not caller_id:
        return jsonify({"error": "Missing caller_id"}), 400
    
    # Test different formats
    formats = {
        "original": caller_id,
        "digits_only": ''.join(filter(str.isdigit, str(caller_id))),
        "without_country_code": ''.join(filter(str.isdigit, str(caller_id)))[1:] if ''.join(filter(str.isdigit, str(caller_id))).startswith('1') else ''.join(filter(str.isdigit, str(caller_id))),
        "with_dashes": None,
        "with_parentheses": None
    }
    
    digits = ''.join(filter(str.isdigit, str(caller_id)))
    if len(digits) == 10:
        formats["with_dashes"] = f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
        formats["with_parentheses"] = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    
    return jsonify({
        "input": caller_id,
        "formats": formats,
        "recommendation": formats["digits_only"][1:] if len(formats["digits_only"]) == 11 and formats["digits_only"].startswith('1') else formats["digits_only"]
    })

# Test MarketCall API directly
@app.route("/test-marketcall-api", methods=["POST"])
def test_marketcall_api():
    """Test MarketCall API with different parameter formats."""
    try:
        data = request.get_json()
        caller_id = data.get("caller_id", "2345678900")
        zip_code = data.get("zip_code", "07004")
        
        # Clean inputs
        digits_only = ''.join(filter(str.isdigit, str(caller_id)))
        zip_digits = ''.join(filter(str.isdigit, str(zip_code)))
        
        # Test different formats
        test_cases = [
            {
                "name": "10_digit_caller_zip_padded",
                "payload": {
                    "campaign_id": CAMPAIGN_ID,
                    "caller_id": digits_only[-10:],  # Last 10 digits
                    "zip_code": zip_digits.zfill(5)  # Zero-padded
                }
            },
            {
                "name": "11_digit_caller_zip_padded", 
                "payload": {
                    "campaign_id": CAMPAIGN_ID,
                    "caller_id": clean_caller_id(caller_id),  # 11 digits with country code
                    "zip_code": zip_digits.zfill(5)
                }
            },
            {
                "name": "10_digit_caller_zip_unpadded",
                "payload": {
                    "campaign_id": CAMPAIGN_ID,
                    "caller_id": digits_only[-10:],
                    "zip_code": zip_digits.lstrip('0') or '0'  # Remove leading zeros safely
                }
            }
        ]
        
        results = []
        headers = {"X-Api-Key": API_KEY, "Content-Type": "application/json; charset=utf-8"}
        
        for test_case in test_cases:
            try:
                logger.info(f"Testing MarketCall API with: {test_case}")
                response = requests.post(
                    f"https://www.marketcall.com/api/v1/affiliate/offers/11558/bid-requests",
                    headers=headers,
                    json=test_case["payload"],
                    timeout=10
                )
                
                results.append({
                    "test": test_case["name"],
                    "payload": test_case["payload"],
                    "status_code": response.status_code,
                    "response": response.text,
                    "success": response.status_code == 200
                })
                
            except Exception as e:
                logger.error(f"Test case {test_case['name']} failed: {e}")
                results.append({
                    "test": test_case["name"],
                    "payload": test_case["payload"],
                    "error": str(e),
                    "success": False
                })
        
        return jsonify({
            "input": {"caller_id": caller_id, "zip_code": zip_code},
            "cleaned": {"digits": digits_only, "zip": zip_digits},
            "results": results
        })
        
    except Exception as e:
        logger.error(f"test_marketcall_api failed: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/", methods=["GET"])
def home():
    return jsonify({"status": "Webhook is running", "version": "2.0"})

@app.route("/health", methods=["GET"]) 
def health_check():
    zip_sets = load_zip_sets()
    return jsonify({
        "status": "healthy",
        "total_zip_codes_loaded": sum(len(zips) for zips in zip_sets.values()),
        "tiers": {tier: len(zips) for tier, zips in zip_sets.items()}
    })

@app.route("/call-event", methods=["POST"])
def handle_call():
    try:
        if not request.is_json:
            return jsonify({"error": "Content-Type must be application/json"}), 400
            
        data = request.get_json()
        if not data:
            return jsonify({"error": "Invalid JSON payload"}), 400
            
        caller_id = data.get("caller_id")
        zip_code_raw = data.get("zip_code")
        
        if not caller_id or not zip_code_raw:
            return jsonify({"error": "Missing caller_id or zip_code"}), 400
            
        zip_code = str(zip_code_raw).strip().zfill(5)
        cleaned_caller_id = clean_caller_id(caller_id)
        logger.info(f"Processing ZIP: {zip_code_raw} -> {zip_code}")
        logger.info(f"Processing caller_id: {caller_id} -> {cleaned_caller_id}")
        
        # Load ZIP sets and find tier
        zip_sets = load_zip_sets()
        tier = None
        
        for t, zips in zip_sets.items():
            if zip_code in zips:
                tier = t
                break
        
        if not tier:
            return jsonify({
                "status": "ZIP code not in any tier — no ping sent",
                "zip_code": zip_code,
                "tier_counts": {t: len(zips) for t, zips in zip_sets.items()}
            }), 200
        
        # Make API request
        offer_id = OFFERS[tier]
        payload = {"campaign_id": CAMPAIGN_ID, "caller_id": cleaned_caller_id, "zip_code": zip_code}
        headers = {"X-Api-Key": API_KEY, "Content-Type": "application/json; charset=utf-8"}
        
        try:
            response = requests.post(
                f"https://www.marketcall.com/api/v1/affiliate/offers/{offer_id}/bid-requests",
                headers=headers, json=payload, timeout=30
            )
            
            logger.info(f"MarketCall API response status: {response.status_code}")
            logger.info(f"MarketCall API response headers: {dict(response.headers)}")
            logger.info(f"MarketCall API response body: {response.text}")
            
            response.raise_for_status()
            
            return jsonify({
                "status": f"ZIP matched {tier.upper()} → Offer {offer_id}",
                "zip_code": zip_code, "tier": tier, "offer_id": offer_id,
                "marketcall_response": response.json()
            }), 200
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            logger.error(f"Response status: {getattr(e.response, 'status_code', 'No response')}")
            logger.error(f"Response text: {getattr(e.response, 'text', 'No response text')}")
            
            # Return detailed error info
            error_details = {
                "error": "MarketCall API request failed",
                "api_url": f"https://www.marketcall.com/api/v1/affiliate/offers/{offer_id}/bid-requests",
                "payload_sent": payload,
                "headers_sent": {k: v for k, v in headers.items() if k != "X-Api-Key"},  # Hide API key
                "status_code": getattr(e.response, 'status_code', None),
                "response_text": getattr(e.response, 'text', None)
            }
            return jsonify(error_details), 502
            
    except Exception as e:
        logger.error(f"Error in handle_call: {e}")
        return jsonify({"error": "Internal server error"}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)