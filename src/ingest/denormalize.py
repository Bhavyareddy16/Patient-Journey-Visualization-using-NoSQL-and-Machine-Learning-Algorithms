# src/ingest/denormalize.py
from pymongo import MongoClient
from tqdm import tqdm
import pandas as pd

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["patient_journey"]

print("🔗 Connected to MongoDB...")

# Load data
patients = list(db.patients.find({}))
encounters = list(db.encounters.find({}))
conditions = list(db.conditions.find({}))
medications = list(db.medications.find({}))
procedures = list(db.procedures.find({}))
observations = list(db.observations.find({}))

print("📦 Data loaded from MongoDB collections:")
print(f"Patients: {len(patients)} | Encounters: {len(encounters)} | Conditions: {len(conditions)}")

# Group encounters by patient
enc_by_patient = {}
for e in encounters:
    pid = e.get("PATIENT")
    enc_by_patient.setdefault(pid, []).append(e)

# Group conditions, medications, etc. by patient
cond_by_patient = {}
for c in conditions:
    pid = c.get("PATIENT")
    cond_by_patient.setdefault(pid, []).append(c)

med_by_patient = {}
for m in medications:
    pid = m.get("PATIENT")
    med_by_patient.setdefault(pid, []).append(m)

proc_by_patient = {}
for p in procedures:
    pid = p.get("PATIENT")
    proc_by_patient.setdefault(pid, []).append(p)

obs_by_patient = {}
for o in observations:
    pid = o.get("PATIENT")
    obs_by_patient.setdefault(pid, []).append(o)

print("🔄 Grouped all patient-related data...")

# Build denormalized documents
denorm_docs = []
for p in tqdm(patients, desc="Merging patient records"):
    pid = p.get("Id")
    combined = {
        "patient_id": pid,
        "demographics": p,
        "encounters": enc_by_patient.get(pid, []),
        "conditions": cond_by_patient.get(pid, []),
        "medications": med_by_patient.get(pid, []),
        "procedures": proc_by_patient.get(pid, []),
        "observations": obs_by_patient.get(pid, []),
    }
    denorm_docs.append(combined)

# Insert into new MongoDB collection
db.patients_denorm.delete_many({})
if denorm_docs:
    db.patients_denorm.insert_many(denorm_docs)

print(f"✅ Denormalization complete! Inserted {len(denorm_docs)} records into 'patients_denorm' collection.")

