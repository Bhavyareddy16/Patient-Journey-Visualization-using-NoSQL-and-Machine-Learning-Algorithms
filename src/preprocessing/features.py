from pymongo import MongoClient
import pandas as pd
import numpy as np
from datetime import datetime

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["patient_journey"]

print("🔗 Connected to MongoDB...")

# Load denormalized patients
patients = list(db.patients_denorm.find({}))
print(f"📦 Loaded {len(patients)} patient records from patients_denorm")

rows = []

# Helper function to safely parse dates
def parse_date(date_str):
    try:
        return pd.to_datetime(date_str)
    except Exception:
        return None

for p in patients:
    pid = p.get("patient_id")
    encounters = p.get("encounters", [])
    if not encounters:
        continue

    # Extract start and stop dates
    starts = [parse_date(e.get("START")) for e in encounters if e.get("START")]
    stops = [parse_date(e.get("STOP")) for e in encounters if e.get("STOP")]
    starts = [s for s in starts if s is not pd.NaT]
    stops = [s for s in stops if s is not pd.NaT]

    if not starts:
        continue

    # Calculate metrics
    num_visits = len(starts)
    journey_start = min(starts)
    journey_end = max(stops) if stops else journey_start
    duration_days = (journey_end - journey_start).days if journey_end and journey_start else 0
    avg_gap_days = duration_days / num_visits if num_visits > 1 else 0

    # Store computed metrics
    rows.append({
        "patient_id": pid,
        "num_visits": num_visits,
        "journey_start": journey_start,
        "journey_end": journey_end,
        "journey_duration_days": duration_days,
        "avg_gap_days": avg_gap_days
    })

# Convert to DataFrame
df = pd.DataFrame(rows)
print(f"✅ Generated features for {len(df)} patients.")

# Save into MongoDB
db.patient_features.delete_many({})
if not df.empty:
    db.patient_features.insert_many(df.to_dict(orient="records"))

print("📊 Saved all features to MongoDB collection 'patient_features'.")


