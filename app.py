from flask import Flask, request, jsonify
import requests
import pandas as pd
import os
from functools import lru_cache
import logging

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === Configuration ===
API_KEY = os.environ.get("API_KEY", "25c787e461c18a4a2a502ce49423a2808a68da65")
CAMPAIGN_ID = "323747"
OFFERS = {
    "tier_1": "11558",
    "tier_2": "22222", 
    "tier_3": "33333"
}

# GitHub raw file URLs
GITHUB_BASE_URL = "https://raw.githubusercontent.com/HannielSolutions/zip-routing-api/main"
SHEET_FILES = {
    "tier_1": f"{GITHUB_BASE_URL}/Tier%201.xlsx",
    "tier_2": f"{GITHUB_BASE_URL}/Tier%202.xlsx", 
    "tier_3": f"{GITHUB_BASE_URL}/Tier%203.xlsx"
}

# === Load ZIPs (with caching and detailed logging) ===
@lru_cache(maxsize=1)
def download_excel_from_github(url):
    """Download Excel file from GitHub."""
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.content
    except Exception as e:
        logger.error(f"Failed to download {url}: {e}")
        return None

def load_zip_sets():
    """Load ZIP codes from Excel files and cache the result."""
    zip_sets = {}
    for tier, url in SHEET_FILES.items():
        try:
            logger.info(f"Downloading {tier} from GitHub...")
            excel_content = download_excel_from_github(url)
            
            if excel_content is None:
                logger.warning(f"Could not download file for {tier}")
                zip_sets[tier] = set()
                continue
            
            # Read Excel from bytes
            import io
            df = pd.read_excel(io.BytesIO(excel_content), usecols=[0], dtype={0: str})
            
            if df.empty:
                logger.warning(f"Empty file for {tier}")
                zip_sets[tier] = set()
                continue
            
            logger.info(f"Raw data from {tier}: {df.iloc[:5, 0].tolist()}")  # Show first 5 rows
            
            zip_list = df.iloc[:, 0].astype(str).str.zfill(5).str.strip()
            zip_list = zip_list.dropna()
            zip_sets[tier] = set(zip_list)
            
            logger.info(f"{tier} ZIPs loaded: {len(zip_sets[tier])}")
            logger.info(f"Sample ZIPs from {tier}: {list(zip_sets[tier])[:10]}")  # Show first 10 ZIPs
            
        except Exception as e:
            logger.error(f"Failed to load ZIPs for {tier}: {e}")
            zip_sets[tier] = set()
    return zip_sets

# === Debug endpoint to check loaded ZIPs ===
@app.route("/debug-zips", methods=["GET"])
def debug_zips():
    """Debug endpoint to see what ZIP codes are loaded."""
    zip_sets = load_zip_sets()
    debug_info = {}
    
    for tier, zips in zip_sets.items():
        debug_info[tier] = {
            "count": len(zips),
            "sample_zips": list(zips)[:20] if zips else [],  # Show first 20
            "contains_07004": "07004" in zips,
            "file_url": SHEET_FILES[tier],
            "source": "GitHub"
        }
    
    return jsonify({
        "debug_info": debug_info,
        "total_zips": sum(len(zips) for zips in zip_sets.values())
    })

# === Check specific ZIP endpoint ===
@app.route("/check-zip/<zip_code>", methods=["GET"])
def check_zip(zip_code):
    """Check which tier a specific ZIP code belongs to."""
    processed_zip = str(zip_code).strip().zfill(5)
    zip_sets = load_zip_sets()
    
    result = {
        "original_zip": zip_code,
        "processed_zip": processed_zip,
        "found_in_tiers": []
    }
    
    for tier, zips in zip_sets.items():
        if processed_zip in zips:
            result["found_in_tiers"].append(tier)
    
    return jsonify(result)

# === Utility Functions ===
def validate_zip_code(zip_code):
    """Validate ZIP code format."""
    if not zip_code:
        return False
    zip_str = str(zip_code).strip()
    padded = zip_str.zfill(5)
    return len(padded) == 5 and padded.isdigit()

def find_tier_for_zip(zip_code, zip_sets):
    """Find which tier a ZIP code belongs to."""
    for tier in ["tier_1", "tier_2", "tier_3"]:
        if zip_code in zip_sets[tier]:
            return tier
    return None

# === Routes ===
@app.route("/", methods=["GET"])
def home():
    return jsonify({"status": "Webhook is running", "version": "1.0"})

@app.route("/health", methods=["GET"]) 
def health_check():
    """Health check endpoint."""
    zip_sets = load_zip_sets()
    total_zips = sum(len(zips) for zips in zip_sets.values())
    return jsonify({
        "status": "healthy",
        "total_zip_codes_loaded": total_zips,
        "tiers": {tier: len(zips) for tier, zips in zip_sets.items()},
        "github_urls": list(SHEET_FILES.values())
    })

@app.route("/call-event", methods=["POST"])
def handle_call():
    """Handle incoming call events and route to appropriate offers."""
    try:
        if not request.is_json:
            return jsonify({"error": "Content-Type must be application/json"}), 400
            
        data = request.get_json()
        if not data:
            return jsonify({"error": "Invalid JSON payload"}), 400
            
        caller_id = data.get("caller_id")
        zip_code_raw = data.get("zip_code")
        
        if not caller_id:
            return jsonify({"error": "Missing caller_id"}), 400
        if not zip_code_raw:
            return jsonify({"error": "Missing zip_code"}), 400
            
        zip_code = str(zip_code_raw).strip().zfill(5)
        logger.info(f"Processing call - Caller ID: {caller_id}, Original ZIP: {zip_code_raw}, Processed ZIP: {zip_code}")
        
        if not validate_zip_code(zip_code):
            return jsonify({"error": "Invalid zip_code format"}), 400
        
        zip_sets = load_zip_sets()
        
        # Enhanced debugging for ZIP lookup
        logger.info(f"Looking for ZIP {zip_code} in loaded sets...")
        for tier, zips in zip_sets.items():
            logger.info(f"{tier}: {len(zips)} ZIPs loaded, contains {zip_code}: {zip_code in zips}")
        
        tier = find_tier_for_zip(zip_code, zip_sets)
        
        if not tier:
            logger.info(f"ZIP {zip_code} not found in any tier")
            # Return more debug info when ZIP not found
            return jsonify({
                "status": "ZIP code not in any tier — no ping sent",
                "zip_code": zip_code,
                "original_zip": zip_code_raw,
                "debug": {
                    "tier_counts": {t: len(zips) for t, zips in zip_sets.items()},
                    "files_checked": list(SHEET_FILES.keys())
                }
            }), 200
        
        # Prepare API request
        offer_id = OFFERS[tier]
        payload = {
            "campaign_id": CAMPAIGN_ID,
            "caller_id": caller_id,
            "zip_code": zip_code
        }
        headers = {
            "X-Api-Key": API_KEY,
            "Content-Type": "application/json; charset=utf-8"
        }
        
        try:
            response = requests.post(
                f"https://www.marketcall.com/api/v1/affiliate/offers/{offer_id}/bid-requests",
                headers=headers,
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            
            logger.info(f"Successfully sent to {tier.upper()}, Offer {offer_id}")
            return jsonify({
                "status": f"ZIP matched {tier.upper()} → Offer {offer_id}",
                "zip_code": zip_code,
                "tier": tier,
                "offer_id": offer_id,
                "marketcall_response": response.json()
            }), 200
            
        except requests.exceptions.Timeout:
            logger.error("API request timed out")
            return jsonify({"error": "API request timed out"}), 504
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            return jsonify({"error": "Failed to send request to MarketCall API"}), 502
        except ValueError as e:
            logger.error(f"Invalid JSON response from API: {e}")
            return jsonify({"error": "Invalid response from MarketCall API"}), 502
            
    except Exception as e:
        logger.error(f"Unexpected error in handle_call: {e}")
        return jsonify({"error": "Internal server error"}), 500

# === Error Handlers ===
@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(405)
def method_not_allowed(error):
    return jsonify({"error": "Method not allowed"}), 405

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500

# === Run Flask ===
if __name__ == "__main__":
    try:
        zip_sets = load_zip_sets()
        total_zips = sum(len(zips) for zips in zip_sets.values())
        logger.info(f"Application starting with {total_zips} ZIP codes loaded")
    except Exception as e:
        logger.error(f"Failed to load ZIP codes on startup: {e}")
    
    app.run(host="0.0.0.0", port=5000, debug=False)