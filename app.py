from flask import Flask, request, jsonify
import requests
import pandas as pd

app = Flask(__name__)

# Constants
API_KEY = "25c787e461c18a4a2a502ce49423a2808a68da65"
CAMPAIGN_ID = "323747"

OFFERS = {
    "tier_1": "11558",  # Update with your actual offer IDs if different
    "tier_2": "22222",
    "tier_3": "33333"
}

EXCEL_FILES = {
    "tier_1": "Tier 1.xlsx",
    "tier_2": "Tier 2.xlsx",
    "tier_3": "Tier 3.xlsx"
}

# Load ZIP codes from Excel files
def load_zip_sets():
    zip_sets = {}
    for tier, filepath in EXCEL_FILES.items():
        try:
            df = pd.read_excel(filepath)
            zip_column = df.columns[0]
            zip_sets[tier] = set(df[zip_column].astype(str).str.strip())
        except Exception as e:
            print(f"Error loading {tier} ZIPs: {e}")
            zip_sets[tier] = set()
    return zip_sets

@app.route("/", methods=["GET"])
def home():
    return "Webhook is running"

@app.route("/call-event", methods=["POST"])
def handle_call():
    data = request.json
    caller_id = data.get("caller_id")
    zip_code = str(data.get("zip_code")).strip()

    if not caller_id or not zip_code:
        return jsonify({"error": "Missing caller_id or zip_code"}), 400

    zip_sets = load_zip_sets()

    if zip_code in zip_sets["tier_1"]:
        tier = "tier_1"
    elif zip_code in zip_sets["tier_2"]:
        tier = "tier_2"
    elif zip_code in zip_sets["tier_3"]:
        tier = "tier_3"
    else:
        return jsonify({
            "status": "ZIP code not in any tier — no ping sent"
        }), 200

    payload = {
        "campaign_id": CAMPAIGN_ID,
        "caller_id": caller_id,
        "zip_code": zip_code
    }

    headers = {
        "X-Api-Key": API_KEY,
        "Content-Type": "application/json; charset=utf-8"
    }

    offer_id = OFFERS[tier]
    response = requests.post(
        f"https://www.marketcall.com/api/v1/affiliate/offers/{offer_id}/bid-requests",
        headers=headers,
        json=payload
    )

    return jsonify({
        "status": f"ZIP matched {tier.upper()} → Offer {offer_id}",
        "marketcall_response": response.json()
    }), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
