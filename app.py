# app.py
"""
Flask API for MGNREGA frontend to consume.
Endpoints:
 - GET  /api/districts                 -> list {id, name}
 - GET  /api/district/<id>/summary     -> monthly array (MGNREGA)
 - GET  /api/district/<id>             -> district info (area, taluks)
 - POST/GET /api/reverse-geocode       -> get district name from lat/lon
 - GET  /api/health                    -> health check

Notes:
 - Falls back to local `districts_data` if DB has no districts.
 - Reverse geocoding uses OpenStreetMap (Nominatim).
"""

import os
import json
import glob
import requests

from dotenv import load_dotenv
from flask import Flask, jsonify, abort, request, send_from_directory
from flask_cors import CORS

# Optional imports for DB and caching
try:
    from db import SessionLocal
    from models import District, MgnregaMonthly
except Exception:
    SessionLocal = None
    District = None
    MgnregaMonthly = None

try:
    import redis
except Exception:
    redis = None

# Local district dataset (external file)
# Ensure backend/districts_data.py exists and defines `districts_data` list
try:
    from districts_data import districts_data
except Exception:
    districts_data = []  # fall back to empty list if file not present

# ------------------ CONFIG ------------------
BASE_DIR = os.path.dirname(__file__)
load_dotenv(dotenv_path=os.path.join(BASE_DIR, ".env"))

app = Flask(__name__, static_folder='../frontend/build', static_url_path='/')
CORS(app)

REDIS_URL = os.getenv('REDIS_URL')
redis_client = None
if redis and REDIS_URL:
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    except Exception:
        redis_client = None

CACHE_TTL = int(os.getenv('CACHE_TTL', 300))
RAW_DIR = os.getenv('RAW_DIR', 'raw_fetches')

# ------------------ HELPERS ------------------
def latest_snapshot_for_district(district_name):
    """
    If DB misses, attempt to find district's data in latest raw JSON snapshot files.
    Returns list of monthly records or None.
    """
    if not os.path.isdir(RAW_DIR):
        return None

    files = sorted(glob.glob(os.path.join(RAW_DIR, "*.json")), reverse=True)
    for f in files[:5]:
        try:
            with open(f, 'r', encoding='utf-8') as fh:
                data = json.load(fh)
            records = data.get('records') or data.get('data') or []
            out = []
            for r in records:
                dn = (r.get('district') or r.get('district_name') or r.get('district_name_en') or "").strip()
                if dn and dn.lower() == district_name.strip().lower():
                    try:
                        out.append({
                            "year": int(r.get('year') or 0),
                            "month": int(r.get('month') or 0),
                            "persons": int(r.get('persons_benefitted') or r.get('persons') or 0),
                            "person_days": int(r.get('person_days') or r.get('persondays') or 0),
                            "wages": int(r.get('wages_paid') or r.get('wages') or 0),
                            "households": int(r.get('households_worked') or r.get('households') or 0)
                        })
                    except Exception:
                        continue
            if out:
                return sorted(out, key=lambda x: (x['year'], x['month']))
        except Exception:
            continue
    return None

def get_db_districts():
    """Return list of {id,name} from DB if available, otherwise empty list."""
    if not SessionLocal or District is None:
        return []
    try:
        with SessionLocal() as s:
            rows = s.query(District.id, District.district_name).order_by(District.district_name).all()
            return [{"id": r[0], "name": r[1]} for r in rows]
    except Exception:
        return []

def get_db_summary(district_id):
    """Return MGNREGA monthly summary list from DB or None."""
    if not SessionLocal or MgnregaMonthly is None:
        return None
    try:
        with SessionLocal() as s:
            rows = s.query(MgnregaMonthly).filter_by(district_id=district_id).order_by(MgnregaMonthly.year, MgnregaMonthly.month).all()
            if not rows:
                return None
            out = []
            for r in rows:
                out.append({
                    "year": r.year,
                    "month": r.month,
                    "persons": int(r.persons_benefitted or 0),
                    "person_days": int(r.person_days or 0),
                    "wages": int(r.wages_paid or 0),
                    "households": int(r.households_worked or 0)
                })
            return out
    except Exception:
        return None

# ------------------ ROUTES ------------------

@app.route('/api/districts', methods=['GET'])
def list_districts():
    """List districts from DB or fallback to local data file."""
    cache_key = "districts_list"
    # Try redis cache first
    if redis_client:
        try:
            cached = redis_client.get(cache_key)
            if cached:
                return jsonify(json.loads(cached))
        except Exception:
            pass

    out = get_db_districts()
    if not out:
        # fallback to districts_data if DB empty
        out = [{"id": d.get("id"), "name": d.get("name")} for d in (districts_data or [])]

    # cache result
    if redis_client:
        try:
            redis_client.set(cache_key, json.dumps(out), ex=CACHE_TTL)
        except Exception:
            pass

    return jsonify(out)

@app.route('/api/district/<int:district_id>/summary', methods=['GET'])
def district_summary(district_id):
    """Return monthly MGNREGA summary for a district (from DB or snapshot fallback)."""
    cache_key = f"district_summary:{district_id}"
    if redis_client:
        try:
            cached = redis_client.get(cache_key)
            if cached:
                return jsonify(json.loads(cached))
        except Exception:
            pass

    # Try DB
    db_out = get_db_summary(district_id)
    if db_out is not None:
        # cache
        if redis_client:
            try:
                redis_client.set(cache_key, json.dumps(db_out), ex=CACHE_TTL)
            except Exception:
                pass
        return jsonify(db_out)

    # DB empty -> try snapshot fallback using district name from DB if available
    # If DB not available, attempt to match from local dataset
    district_name = None
    if SessionLocal and District:
        try:
            with SessionLocal() as s:
                d = s.query(District).filter_by(id=district_id).first()
                if d:
                    district_name = d.district_name
        except Exception:
            district_name = None

    if not district_name and districts_data:
        # try find name in local dataset (by id)
        match = next((d for d in districts_data if d.get("id") == district_id), None)
        if match:
            district_name = match.get("name")

    if district_name:
        snap = latest_snapshot_for_district(district_name)
        if snap:
            return jsonify(snap)

    return abort(404)

@app.route('/api/health', methods=['GET'])
def health():
    return jsonify({"status": "ok"})

# ------------------ Reverse Geocode ------------------
@app.route("/api/reverse-geocode", methods=["POST", "GET"])
def reverse_geocode():
    """
    Accepts:
      - POST JSON: { "lat": 13.08, "lon": 80.27 }
      - GET query: /api/reverse-geocode?lat=13.08&lon=80.27
    Returns: { "district": "Chennai" } or { "error": "..." }
    """
    try:
        data = request.get_json(silent=True) or {}
        lat = data.get("lat") or request.args.get("lat")
        lon = data.get("lon") or request.args.get("lon")

        if not lat or not lon:
            return jsonify({"error": "Missing lat/lon"}), 400

        # Call Nominatim reverse geocode
        url = f"https://nominatim.openstreetmap.org/reverse?lat={lat}&lon={lon}&format=json&addressdetails=1"
        resp = requests.get(url, headers={"User-Agent": "mgnrega-tn-app/1.0"}, timeout=10)
        if resp.status_code != 200:
            return jsonify({"error": "Reverse geocode failed"}), 502

        body = resp.json()
        address = body.get("address", {})

        # Try common fields for district-like values
        district = (
            address.get("county")
            or address.get("state_district")
            or address.get("region")
            or address.get("district")
            or address.get("city")
            or address.get("town")
        )

        if not district:
            return jsonify({"error": "District not found"}), 404

        return jsonify({"district": district})
    except requests.exceptions.RequestException as e:
        return jsonify({"error": f"Reverse geocode request failed: {str(e)}"}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ------------------ District + Taluk Details ------------------
@app.route("/api/district/<int:district_id>", methods=["GET"])
def get_district_details(district_id):
    """
    Return full district details (area, taluks, etc.) from local dataset.
    Requires backend/districts_data.py to define `districts_data` list.
    """
    if not districts_data:
        return jsonify({"error": "District data not available on server"}), 404

    district = next((d for d in districts_data if int(d.get("id")) == int(district_id)), None)
    if district:
        return jsonify(district)
    return jsonify({"error": "District not found"}), 404

# ------------------ Serve React Frontend ------------------
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve_frontend(path):
    # Serve built React app when available
    if path != "" and os.path.exists(os.path.join(app.static_folder, path)):
        return send_from_directory(app.static_folder, path)
    return send_from_directory(app.static_folder, 'index.html')

# ------------------ MAIN ------------------
if __name__ == "__main__":
    port = int(os.getenv('PORT', 8000))
    # debug=True is convenient for local development; disable in production
    app.run(host="0.0.0.0", port=port, debug=True)
