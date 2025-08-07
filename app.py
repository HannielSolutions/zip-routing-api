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

# Tier configuration with business hours and fallback
TIER_CONFIG = {
    "tier_1": {
        "offer_id": "11558",
        "hours": {"start": 9, "end": 21},  # 9 AM - 9 PM
        "timezone": "US/Eastern",
        "max_calls_per_hour": 100,
        "fallback_tier": "tier_2"
    },
    "tier_2": {
        "offer_id": "11558",  # Update with real offer ID when available
        "hours": {"start": 8, "end": 22},  # 8 AM - 10 PM
        "timezone": "US/Central", 
        "max_calls_per_hour": 80,
        "fallback_tier": "tier_3"
    },
    "tier_3": {
        "offer_id": "11558",  # Update with real offer ID when available
        "hours": {"start": 7, "end": 23},  # 7 AM - 11 PM
        "timezone": "US/Pacific",
        "max_calls_per_hour": 60,
        "fallback_tier": None
    }
}

# Excel file URL
EXCEL_URL = "https://raw.githubusercontent.com/HannielSolutions/zip-routing-api/main/Tier%201.xlsx"

# Global data storage
_zip_cache = None
_call_history = deque(maxlen=10000)  # Keep last 10,000 calls in memory
_call_counts = defaultdict(lambda: defaultdict(int))  # tier -> hour -> count
_analytics = {
    "total_calls": 0,
    "successful_calls": 0,
    "failed_calls": 0,
    "tier_stats": defaultdict(int),
    "hourly_stats": defaultdict(int),
    "zip_stats": defaultdict(int)
}

# CSV file path
CSV_FILE = "call_logs.csv"

# Thread lock for thread-safe operations
data_lock = threading.Lock()

def init_csv_file():
    """Initialize CSV file with headers if it doesn't exist."""
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                'timestamp', 'caller_id', 'zip_code', 'tier', 'offer_id',
                'status', 'response_time_ms', 'marketcall_id', 'is_available',
                'fallback_used', 'business_hours_check', 'rate_limit_check'
            ])

def log_call_to_csv(call_data):
    """Log call data to CSV file."""
    try:
        with open(CSV_FILE, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                call_data.get('timestamp'),
                call_data.get('caller_id'),
                call_data.get('zip_code'),
                call_data.get('tier'),
                call_data.get('offer_id'),
                call_data.get('status'),
                call_data.get('response_time_ms'),
                call_data.get('marketcall_id'),
                call_data.get('is_available'),
                call_data.get('fallback_used', False),
                call_data.get('business_hours_check', True),
                call_data.get('rate_limit_check', True)
            ])
    except Exception as e:
        logger.error(f"Failed to log to CSV: {e}")

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
        tier_data = {"tier_1": set(), "tier_2": set(), "tier_3": set()}
        
        for _, row in df.iterrows():
            if pd.notna(row['Zip Code']) and pd.notna(row['PriceTier']):
                zip_code = str(int(row['Zip Code'])).zfill(5)
                tier_name = row['PriceTier'].replace(' ', '_').lower()
                
                if tier_name in tier_data:
                    tier_data[tier_name].add(zip_code)
        
        logger.info(f"Loaded ZIP counts: {[(t, len(zips)) for t, zips in tier_data.items()]}")
        _zip_cache = tier_data
        return tier_data
        
    except Exception as e:
        logger.error(f"Failed to load ZIP data: {e}")
        return {"tier_1": set(), "tier_2": set(), "tier_3": set()}

def is_business_hours(tier):
    """Check if current time is within business hours for a tier."""
    try:
        config = TIER_CONFIG[tier]
        tz = pytz.timezone(config["timezone"])
        current_time = datetime.now(tz)
        current_hour = current_time.hour
        
        return config["hours"]["start"] <= current_hour < config["hours"]["end"]
    except Exception as e:
        logger.error(f"Error checking business hours for {tier}: {e}")
        return True  # Default to open if error

def check_rate_limit(tier):
    """Check if tier has exceeded hourly call limit."""
    try:
        current_hour = datetime.now().hour
        current_count = _call_counts[tier][current_hour]
        max_calls = TIER_CONFIG[tier]["max_calls_per_hour"]
        
        return current_count < max_calls
    except Exception as e:
        logger.error(f"Error checking rate limit for {tier}: {e}")
        return True  # Default to allow if error

def get_best_tier(original_tier):
    """Get the best available tier considering business hours and rate limits."""
    # Try original tier first
    if is_business_hours(original_tier) and check_rate_limit(original_tier):
        return original_tier, False
    
    # Try fallback chain
    current_tier = original_tier
    while current_tier:
        fallback_tier = TIER_CONFIG[current_tier].get("fallback_tier")
        if fallback_tier and is_business_hours(fallback_tier) and check_rate_limit(fallback_tier):
            return fallback_tier, True
        current_tier = fallback_tier
    
    # If no tier is available, use original tier anyway (business decision)
    return original_tier, False

def update_analytics(call_data):
    """Update real-time analytics."""
    with data_lock:
        _analytics["total_calls"] += 1
        if call_data["status"] == "success":
            _analytics["successful_calls"] += 1
        else:
            _analytics["failed_calls"] += 1
        
        _analytics["tier_stats"][call_data["tier"]] += 1
        _analytics["hourly_stats"][datetime.now().hour] += 1
        _analytics["zip_stats"][call_data["zip_code"]] += 1
        
        # Update rate limiting counter
        current_hour = datetime.now().hour
        _call_counts[call_data["tier"]][current_hour] += 1
        
        # Add to history
        _call_history.append(call_data)

# Initialize CSV on startup
try:
    init_csv_file()
except Exception as e:
    logger.error(f"Failed to initialize CSV file: {e}")

@app.route("/", methods=["GET"])
def home():
    return jsonify({"status": "Advanced Call Routing System", "version": "4.0"})

@app.route("/dashboard")
def dashboard():
    """Admin dashboard with real-time analytics."""
    dashboard_html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Call Routing Dashboard</title>
        <meta http-equiv="refresh" content="30">
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }
            .container { max-width: 1200px; margin: 0 auto; }
            .card { background: white; padding: 20px; margin: 10px 0; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
            .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; }
            .stat-card { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; }
            .success { background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%); }
            .error { background: linear-gradient(135deg, #ff6b6b 0%, #ee5a52 100%); }
            .tier1 { background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); }
            .tier2 { background: linear-gradient(135deg, #43e97b 0%, #38f9d7 100%); }
            .tier3 { background: linear-gradient(135deg, #fa709a 0%, #fee140 100%); }
            table { width: 100%; border-collapse: collapse; }
            th, td { padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }
            th { background-color: #f8f9fa; }
            .status-success { color: #28a745; font-weight: bold; }
            .status-error { color: #dc3545; font-weight: bold; }
            .business-hours { color: #28a745; }
            .after-hours { color: #ffc107; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📞 Call Routing Dashboard</h1>
            <p>Last updated: {{ current_time }} | Auto-refresh every 30 seconds</p>
            
            <div class="stats-grid">
                <div class="card stat-card">
                    <h3>📊 Total Calls</h3>
                    <h2>{{ analytics.total_calls }}</h2>
                </div>
                <div class="card stat-card success">
                    <h3>✅ Successful</h3>
                    <h2>{{ analytics.successful_calls }}</h2>
                    <p>{{ success_rate }}% success rate</p>
                </div>
                <div class="card stat-card error">
                    <h3>❌ Failed</h3>
                    <h2>{{ analytics.failed_calls }}</h2>
                </div>
            </div>
            
            <div class="stats-grid">
                <div class="card stat-card tier1">
                    <h3>🏆 Tier 1 Calls</h3>
                    <h2>{{ analytics.tier_stats.tier_1 }}</h2>
                    <p>Business Hours: {{ tier_status.tier_1.business_hours }}</p>
                    <p>Rate Limit: {{ tier_status.tier_1.calls_this_hour }}/{{ tier_status.tier_1.max_calls }}</p>
                </div>
                <div class="card stat-card tier2">
                    <h3>🥈 Tier 2 Calls</h3>
                    <h2>{{ analytics.tier_stats.tier_2 }}</h2>
                    <p>Business Hours: {{ tier_status.tier_2.business_hours }}</p>
                    <p>Rate Limit: {{ tier_status.tier_2.calls_this_hour }}/{{ tier_status.tier_2.max_calls }}</p>
                </div>
                <div class="card stat-card tier3">
                    <h3>🥉 Tier 3 Calls</h3>
                    <h2>{{ analytics.tier_stats.tier_3 }}</h2>
                    <p>Business Hours: {{ tier_status.tier_3.business_hours }}</p>
                    <p>Rate Limit: {{ tier_status.tier_3.calls_this_hour }}/{{ tier_status.tier_3.max_calls }}</p>
                </div>
            </div>
            
            <div class="card">
                <h3>📈 Recent Calls (Last 20)</h3>
                <table>
                    <thead>
                        <tr>
                            <th>Time</th>
                            <th>Caller ID</th>
                            <th>ZIP</th>
                            <th>Tier</th>
                            <th>Status</th>
                            <th>Response Time</th>
                            <th>Fallback Used</th>
                            <th>Business Hours</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for call in recent_calls %}
                        <tr>
                            <td>{{ call.timestamp }}</td>
                            <td>{{ call.caller_id }}</td>
                            <td>{{ call.zip_code }}</td>
                            <td>{{ call.tier }}</td>
                            <td class="status-{{ call.status }}">{{ call.status }}</td>
                            <td>{{ call.response_time_ms }}ms</td>
                            <td>{{ "Yes" if call.fallback_used else "No" }}</td>
                            <td class="{% if call.business_hours_check %}business-hours{% else %}after-hours{% endif %}">
                                {{ "Yes" if call.business_hours_check else "No" }}
                            </td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
            
            <div class="card">
                <h3>⚙️ System Status</h3>
                <p><strong>ZIP Codes Loaded:</strong> {{ zip_counts.total }}</p>
                <p><strong>Tier 1 ZIPs:</strong> {{ zip_counts.tier_1 }}</p>
                <p><strong>Tier 2 ZIPs:</strong> {{ zip_counts.tier_2 }}</p>
                <p><strong>Tier 3 ZIPs:</strong> {{ zip_counts.tier_3 }}</p>
                <p><strong>Current Hour:</strong> {{ current_hour }}</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    try:
        # Prepare data for dashboard
        zip_data = load_all_zip_data()
        current_hour = datetime.now().hour
        
        # Calculate success rate
        total = _analytics["total_calls"]
        success_rate = round((_analytics["successful_calls"] / total * 100) if total > 0 else 0, 1)
        
        # Get tier status
        tier_status = {}
        for tier in TIER_CONFIG:
            tier_status[tier] = {
                "business_hours": "✅ Open" if is_business_hours(tier) else "⏰ Closed",
                "calls_this_hour": _call_counts[tier][current_hour],
                "max_calls": TIER_CONFIG[tier]["max_calls_per_hour"]
            }
        
        # Recent calls (last 20)
        recent_calls = list(_call_history)[-20:]
        recent_calls.reverse()  # Show newest first
        
        return render_template_string(dashboard_html,
            current_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            analytics=_analytics,
            success_rate=success_rate,
            tier_status=tier_status,
            recent_calls=recent_calls,
            current_hour=current_hour,
            zip_counts={
                "total": sum(len(zips) for zips in zip_data.values()),
                "tier_1": len(zip_data["tier_1"]),
                "tier_2": len(zip_data["tier_2"]), 
                "tier_3": len(zip_data["tier_3"])
            }
        )
    except Exception as e:
        logger.error(f"Dashboard error: {e}")
        return f"Dashboard Error: {str(e)}", 500

@app.route("/analytics", methods=["GET"])
def get_analytics():
    """API endpoint for real-time analytics."""
    try:
        return jsonify({
            "analytics": dict(_analytics),
            "call_counts_by_hour": dict(_call_counts),
            "recent_calls_count": len(_call_history),
            "tier_status": {
                tier: {
                    "business_hours": is_business_hours(tier),
                    "rate_limit_ok": check_rate_limit(tier),
                    "calls_this_hour": _call_counts[tier][datetime.now().hour]
                }
                for tier in TIER_CONFIG
            }
        })
    except Exception as e:
        logger.error(f"Analytics error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/health", methods=["GET"]) 
def health_check():
    """Enhanced health check with tier status."""
    try:
        zip_data = load_all_zip_data()
        return jsonify({
            "status": "healthy",
            "version": "4.0",
            "total_zip_codes_loaded": sum(len(zips) for zips in zip_data.values()),
            "tiers": {tier: len(zips) for tier, zips in zip_data.items()},
            "tier_status": {
                tier: {
                    "business_hours": is_business_hours(tier),
                    "rate_limit_ok": check_rate_limit(tier)
                }
                for tier in TIER_CONFIG
            },
            "total_calls": _analytics["total_calls"],
            "success_rate": round((_analytics["successful_calls"] / _analytics["total_calls"] * 100) if _analytics["total_calls"] > 0 else 0, 1)
        })
    except Exception as e:
        logger.error(f"Health check error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/call-event", methods=["POST"])
def handle_call():
    """Enhanced call handler with all advanced features."""
    start_time = time.time()
    call_data = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": "error",
        "fallback_used": False,
        "business_hours_check": True,
        "rate_limit_check": True,
        "response_time_ms": 0
    }
    
    try:
        # Parse input
        data = request.get_json()
        if not data:
            call_data.update({"status": "error", "error": "Invalid JSON"})
            log_call_to_csv(call_data)
            update_analytics(call_data)
            return jsonify({"error": "Invalid JSON"}), 400
            
        caller_id_raw = data.get("caller_id")
        zip_code_raw = data.get("zip_code")
        
        if not caller_id_raw or not zip_code_raw:
            call_data.update({"status": "error", "error": "Missing parameters"})
            log_call_to_csv(call_data)
            update_analytics(call_data)
            return jsonify({"error": "Missing caller_id or zip_code"}), 400
        
        # Process inputs
        zip_code = str(zip_code_raw).strip().zfill(5)
        caller_digits = ''.join(c for c in str(caller_id_raw) if c.isdigit())
        caller_id = '1' + caller_digits if len(caller_digits) == 10 else caller_digits
        
        call_data.update({
            "caller_id": caller_id,
            "zip_code": zip_code
        })
        
        # Find tier
        zip_data = load_all_zip_data()
        original_tier = None
        for t, zips in zip_data.items():
            if zip_code in zips:
                original_tier = t
                break
        
        if not original_tier:
            call_data.update({"status": "no_tier", "tier": "none"})
            call_data["response_time_ms"] = round((time.time() - start_time) * 1000)
            log_call_to_csv(call_data)
            update_analytics(call_data)
            return jsonify({
                "status": "ZIP code not in any tier — no ping sent",
                "zip_code": zip_code,
                "debug": {t: len(zips) for t, zips in zip_data.items()}
            }), 200
        
        # Apply advanced routing logic
        best_tier, fallback_used = get_best_tier(original_tier)
        call_data.update({
            "tier": best_tier,
            "fallback_used": fallback_used,
            "business_hours_check": is_business_hours(best_tier),
            "rate_limit_check": check_rate_limit(best_tier)
        })
        
        # Get offer configuration
        offer_id = TIER_CONFIG[best_tier]["offer_id"]
        call_data["offer_id"] = offer_id
        
        # Call MarketCall API
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
        
        # Calculate response time
        response_time_ms = round((time.time() - start_time) * 1000)
        call_data["response_time_ms"] = response_time_ms
        
        if response.status_code in [200, 201]:
            response_data = response.json()
            call_data.update({
                "status": "success",
                "marketcall_id": response_data.get("data", {}).get("id"),
                "is_available": response_data.get("data", {}).get("is_available", False)
            })
            
            # Log and update analytics
            log_call_to_csv(call_data)
            update_analytics(call_data)
            
            return jsonify({
                "status": f"SUCCESS: ZIP matched {best_tier.upper()} → Offer {offer_id}",
                "zip_code": zip_code,
                "tier": best_tier,
                "original_tier": original_tier,
                "offer_id": offer_id,
                "caller_id_used": caller_id,
                "fallback_used": fallback_used,
                "business_hours": call_data["business_hours_check"],
                "response_time_ms": response_time_ms,
                "marketcall_response": response_data,
                "http_status": response.status_code
            }), 200
        else:
            call_data.update({
                "status": "api_error",
                "marketcall_id": None,
                "is_available": None
            })
            
            log_call_to_csv(call_data)
            update_analytics(call_data)
            
            return jsonify({
                "error": "MarketCall API error",
                "status_code": response.status_code,
                "response": response.text,
                "payload": payload,
                "tier": best_tier,
                "fallback_used": fallback_used,
                "response_time_ms": response_time_ms
            }), 502
            
    except Exception as e:
        call_data.update({
            "status": "exception",
            "response_time_ms": round((time.time() - start_time) * 1000)
        })
        log_call_to_csv(call_data)
        update_analytics(call_data)
        
        logger.error(f"Exception in handle_call: {e}")
        return jsonify({
            "error": "Exception occurred",
            "message": str(e),
            "type": str(type(e).__name__)
        }), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)