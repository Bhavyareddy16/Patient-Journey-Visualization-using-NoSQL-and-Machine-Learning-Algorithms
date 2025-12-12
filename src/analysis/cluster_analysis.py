# ==========================================
# Patient Journey Cluster Analysis Script
# ==========================================

import pandas as pd
from pymongo import MongoClient
import matplotlib.pyplot as plt
import seaborn as sns
import os

# 1. Connect to MongoDB
try:
    client = MongoClient("mongodb://localhost:27017/")
    db = client["patient_journey"]
    print("Connected to MongoDB successfully.")
except Exception as e:
    print(f"Error connecting to MongoDB: {e}")
    exit()

# 2. Load data from patient_clusters collection
try:
    df = pd.DataFrame(list(db["patient_clusters"].find()))
    print(f"Loaded {len(df)} records from 'patient_clusters' collection.")
except Exception as e:
    print(f"Error loading data: {e}")
    client.close()
    exit()

# 3. Generate pairplot visualization
sns.set(style="whitegrid")
pairplot = sns.pairplot(
    df,
    vars=["num_visits", "journey_duration_days", "avg_gap_days"],
    hue="cluster",
    palette="tab10"
)

# Ensure results directory exists
os.makedirs("results", exist_ok=True)

# Save the pairplot image
pairplot_filename = "results/cluster_pairplot.png"
plt.savefig(pairplot_filename, dpi=300)
plt.close()
print(f"Pairplot saved to: {pairplot_filename}")

# 4. Create a summary of clusters
summary = df.groupby("cluster")[["num_visits", "journey_duration_days", "avg_gap_days"]].agg(
    ["count", "mean"]
)
summary_filename = "results/cluster_summary.csv"
summary.to_csv(summary_filename)
print(f"Cluster summary saved to: {summary_filename}")

# 5. Print the summary in terminal
print("\nCluster Counts and Mean Feature Values:")
print(summary.round(2))

# 6. Close MongoDB connection
client.close()
print("\nAnalysis script finished successfully.")


