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
SHEET_FILES = {
    "tier_1": f"{GITHUB_BASE_URL}/Tier%201.xlsx",
    "tier_2": f"{GITHUB_BASE_URL}/Tier%202.xlsx", 
    "tier_3": f"{GITHUB_BASE_URL}/Tier%203.xlsx"
}

def download_and_process_excel(url, tier):
    """Download and process Excel file in one go."""
    try:
        logger.info(f"Downloading {tier} from {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        logger.info(f"Downloaded {len(response.content)} bytes for {tier}")
        
        # Try multiple reading approaches
        content = io.BytesIO(response.content)
        
        # Approach 1: Read all data, no headers
        try:
            logger.info(f"Trying approach 1 for {tier}")
            df = pd.read_excel(content, header=None)
            logger.info(f"Read Excel: shape {df.shape}")
            
            # Get all values from first column as strings
            all_values = df.iloc[:, 0].astype(str).tolist()
            logger.info(f"All values in first column: {all_values[:20]}")
            
            # Filter for ZIP codes (5-digit numbers, including those that start with 0)
            zip_codes = []
            for val in all_values:
                # Clean the value
                cleaned = str(val).strip()
                # Check if it's a valid ZIP code (3-5 digits)
                if cleaned.isdigit() and 3 <= len(cleaned) <= 5:
                    # Pad to 5 digits
                    padded = cleaned.zfill(5)
                    zip_codes.append(padded)
                    logger.info(f"Found ZIP: {cleaned} -> {padded}")
            
            logger.info(f"Extracted {len(zip_codes)} ZIP codes for {tier}")
            logger.info(f"First 20 ZIP codes: {zip_codes[:20]}")
            logger.info(f"Contains 07004: {'07004' in zip_codes}")
            
            return set(zip_codes)
            
        except Exception as e:
            logger.error(f"Approach 1 failed for {tier}: {e}")
            return set()
            
    except Exception as e:
        logger.error(f"Failed to process {tier}: {e}")
        return set()

@lru_cache(maxsize=1)
def load_zip_sets():
    """Load ZIP codes from all Excel files."""
    zip_sets = {}
    for tier, url in SHEET_FILES.items():
        zip_sets[tier] = download_and_process_excel(url, tier)
        logger.info(f"Loaded {len(zip_sets[tier])} ZIPs for {tier}")
    return zip_sets

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