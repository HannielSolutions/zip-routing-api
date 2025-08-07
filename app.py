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
        logger.info(f"Processing ZIP: {zip_code_raw} -> {zip_code}")
        
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
        payload = {"campaign_id": CAMPAIGN_ID, "caller_id": caller_id, "zip_code": zip_code}
        headers = {"X-Api-Key": API_KEY, "Content-Type": "application/json; charset=utf-8"}
        
        try:
            response = requests.post(
                f"https://www.marketcall.com/api/v1/affiliate/offers/{offer_id}/bid-requests",
                headers=headers, json=payload, timeout=30
            )
            response.raise_for_status()
            
            return jsonify({
                "status": f"ZIP matched {tier.upper()} → Offer {offer_id}",
                "zip_code": zip_code, "tier": tier, "offer_id": offer_id,
                "marketcall_response": response.json()
            }), 200
            
        except Exception as e:
            logger.error(f"API request failed: {e}")
            return jsonify({"error": "MarketCall API request failed"}), 502
            
    except Exception as e:
        logger.error(f"Error in handle_call: {e}")
        return jsonify({"error": "Internal server error"}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)