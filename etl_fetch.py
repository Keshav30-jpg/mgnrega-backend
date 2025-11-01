# etl_fetch.py
"""
Run periodically (cron or systemd timer).
Fetch MGNREGA data for STATE_NAME from data.gov.in API,
persist raw JSON snapshots and upsert into Postgres.
"""

import os, json, time
from dotenv import load_dotenv
import requests
from db import engine, SessionLocal
from models import District, MgnregaMonthly, Base
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

API_URL = os.getenv('MGNREGA_API_URL')
API_KEY = os.getenv('MGNREGA_API_KEY')
STATE_NAME = os.getenv('STATE_NAME', 'Tamil Nadu')
RAW_DIR = os.getenv('RAW_DIR', 'raw_fetches')

os.makedirs(RAW_DIR, exist_ok=True)

def fetch_state(state_name):
    params = {
        'api-key': API_KEY,
        'filters[state_name]': state_name,
        'format': 'json',
        'limit': 10000
    }
    r = requests.get(API_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def persist_raw(data):
    fname = os.path.join(RAW_DIR, f"{int(time.time())}.json")
    with open(fname, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print("Saved raw snapshot:", fname)
    return fname

def upsert_records(records):
    """
    records: list of dicts depending on API shape.
    You may need to adapt field names depending on actual API response.
    """
    created = 0
    with SessionLocal() as session:
        for r in records:
            district_name = r.get('district') or r.get('district_name') or r.get('district_name_en')
            if not district_name:
                continue
            try:
                year = int(r.get('year') or 0)
                month = int(r.get('month') or 0)
            except (ValueError, TypeError):
                continue
            if year == 0 or month == 0:
                continue
            persons = int(r.get('persons_benefitted') or r.get('persons') or 0)
            person_days = int(r.get('person_days') or r.get('persondays') or 0)
            wages = int(r.get('wages_paid') or r.get('wages') or 0)
            households = int(r.get('households_worked') or r.get('households') or 0)

            district = session.query(District).filter_by(district_name=district_name, state_name=STATE_NAME).first()
            if not district:
                district = District(state_name=STATE_NAME, district_name=district_name)
                session.add(district)
                session.flush()

            existing = session.query(MgnregaMonthly).filter_by(district_id=district.id, year=year, month=month).first()
            if existing:
                existing.persons_benefitted = persons
                existing.person_days = person_days
                existing.wages_paid = wages
                existing.households_worked = households
                existing.raw_json = r
            else:
                m = MgnregaMonthly(
                    district_id=district.id, year=year, month=month,
                    persons_benefitted=persons, person_days=person_days,
                    wages_paid=wages, households_worked=households, raw_json=r
                )
                session.add(m)
                created += 1
        session.commit()
    print(f"Upsert complete. New rows added: {created}")

def main():
    print("ETL start:", datetime.utcnow().isoformat())
    try:
        data = fetch_state(STATE_NAME)
    except Exception as e:
        print("Fetch failed:", e)
        return
    persist_raw(data)
    records = data.get('records') or data.get('data') or []
    try:
        upsert_records(records)
    except SQLAlchemyError as e:
        print("DB upsert error:", e)

if __name__ == "__main__":
    main()
