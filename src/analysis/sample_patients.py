# ==========================================
# Export Sample Patients per Cluster
# ==========================================

import pandas as pd
from pymongo import MongoClient
import os

# 1. Connect to MongoDB
try:
    client = MongoClient("mongodb://localhost:27017/")
    db = client["patient_journey"]
    print("Connected to MongoDB successfully.")
except Exception as e:
    print(f"Error connecting to MongoDB: {e}")
    exit()

# 2. Load data from patient_clusters
try:
    df = pd.DataFrame(list(db["patient_clusters"].find()))
    print(f"Loaded {len(df)} records from 'patient_clusters' collection.")
except Exception as e:
    print(f"Error loading data: {e}")
    client.close()
    exit()

# 3. Select one sample patient per cluster
sample_df = (
    df.groupby("cluster")
    .apply(lambda x: x.sample(1, random_state=42))
    .reset_index(drop=True)
)

# 4. Select relevant columns for export
sample_df = sample_df[["cluster", "patient_id", "num_visits", "journey_duration_days", "avg_gap_days"]]

# 5. Save to CSV
os.makedirs("results", exist_ok=True)
output_file = "results/sample_patients.csv"
sample_df.to_csv(output_file, index=False)
print(f"Sample patients saved to: {output_file}")

# 6. Print table in terminal
print("\nSample Patients (One per Cluster):")
print(sample_df)

# 7. Close MongoDB connection
client.close()
print("\nScript finished successfully.")

