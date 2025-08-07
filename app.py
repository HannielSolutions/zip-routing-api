from flask import Flask, request, jsonify, render_template_string
import requests
import pandas as pd
import os
import io
import logging
import csv
from datetime import datetime, timedelta
import pytz
from collections import defaultdict, deque
import threading
import time

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
API_KEY = os.environ.get("API_KEY", "25c787e461c18a4a2a502ce49423a2808a68da65")
CAMPAIGN_ID = "323747"
# You need to replace these with your actual MarketCall offer IDs
OFFERS = {
    "tier_1": "11558",    # ✅ This one works - keep it
    "tier_2": "11558",    # 🔧 TEMPORARY: Use same offer until you get real ID
    "tier_3": "11558"     # 🔧 TEMPORARY: Use same offer until you get real ID
}

# All data is in tier 1 file
EXCEL_URL = "https://raw.githubusercontent.com/HannielSolutions/zip-routing-api/main/Tier%201.xlsx"

# Global variable to cache ZIP data
_zip_cache = None

def load_all_zip_data():
    """Load all ZIP code data from the Excel file."""
    global _zip_cache
    if _zip_cache is not None:
        return _zip_cache
    
    try:
        logger.info("Loading ZIP data from Excel")
        response = requests.get(EXCEL_URL, timeout=30)
        response.raise_for_status()
        
        df = pd.read_excel(io.BytesIO(response.content))
        logger.info(f"Loaded Excel with shape: {df.shape}")
        
        # Initialize tier sets
        tier_data = {"tier_1": set(), "tier_2": set(), "tier_3": set()}
        
        # Process each row
        for _, row in df.iterrows():
            if pd.notna(row['Zip Code']) and pd.notna(row['PriceTier']):
                zip_code = str(int(row['Zip Code'])).zfill(5)
                tier_name = row['PriceTier'].replace(' ', '_').lower()  # "Tier 1" -> "tier_1"
                
                if tier_name in tier_data:
                    tier_data[tier_name].add(zip_code)
        
        logger.info(f"Loaded ZIP counts: {[(t, len(zips)) for t, zips in tier_data.items()]}")
        _zip_cache = tier_data
        return tier_data
        
    except Exception as e:
        logger.error(f"Failed to load ZIP data: {e}")
        return {"tier_1": set(), "tier_2": set(), "tier_3": set()}

@app.route("/", methods=["GET"])
def home():
    return jsonify({"status": "Webhook is running", "version": "3.0"})

@app.route("/health", methods=["GET"]) 
def health_check():
    zip_data = load_all_zip_data()
    return jsonify({
        "status": "healthy",
        "total_zip_codes_loaded": sum(len(zips) for zips in zip_data.values()),
        "tiers": {tier: len(zips) for tier, zips in zip_data.items()}
    })

@app.route("/debug/<zip_code>", methods=["GET"])
def debug_zip(zip_code):
    """Debug endpoint to check a specific ZIP code."""
    processed_zip = str(zip_code).strip().zfill(5)
    zip_data = load_all_zip_data()
    
    result = {"zip_code": processed_zip, "found_in": []}
    for tier, zips in zip_data.items():
        if processed_zip in zips:
            result["found_in"].append(tier)
    
    return jsonify(result)

@app.route("/call-event", methods=["POST"])
def handle_call():
    try:
        # Parse input
        data = request.get_json()
        if not data:
            return jsonify({"error": "Invalid JSON"}), 400
            
        caller_id_raw = data.get("caller_id")
        zip_code_raw = data.get("zip_code")
        
        if not caller_id_raw or not zip_code_raw:
            return jsonify({"error": "Missing caller_id or zip_code"}), 400
        
        # Process inputs
        zip_code = str(zip_code_raw).strip().zfill(5)
        
        # Clean caller_id - just digits, add country code if needed
        caller_digits = ''.join(c for c in str(caller_id_raw) if c.isdigit())
        if len(caller_digits) == 10:
            caller_id = '1' + caller_digits
        else:
            caller_id = caller_digits
        
        # Find tier
        zip_data = load_all_zip_data()
        tier = None
        for t, zips in zip_data.items():
            if zip_code in zips:
                tier = t
                break
        
        if not tier:
            return jsonify({
                "status": "ZIP code not in any tier — no ping sent",
                "zip_code": zip_code,
                "debug": {t: len(zips) for t, zips in zip_data.items()}
            }), 200
        
        # Call MarketCall API
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
        
        response = requests.post(
            f"https://www.marketcall.com/api/v1/affiliate/offers/{offer_id}/bid-requests",
            headers=headers,
            json=payload,
            timeout=30
        )
        
        # Success codes: 200 (OK) and 201 (Created)
        if response.status_code in [200, 201]:
            return jsonify({
                "status": f"SUCCESS: ZIP matched {tier.upper()} → Offer {offer_id}",
                "zip_code": zip_code,
                "tier": tier,
                "offer_id": offer_id,
                "caller_id_used": caller_id,
                "marketcall_response": response.json(),
                "http_status": response.status_code
            }), 200
        else:
            return jsonify({
                "error": "MarketCall API error",
                "status_code": response.status_code,
                "response": response.text,
                "payload": payload,
                "caller_id_used": caller_id
            }), 502
            
    except Exception as e:
        return jsonify({
            "error": "Exception occurred",
            "message": str(e),
            "type": str(type(e).__name__)
        }), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)