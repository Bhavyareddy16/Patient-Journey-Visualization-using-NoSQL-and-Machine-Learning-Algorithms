import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["patient_journey"]
collection = db["patient_clusters"]

print("Connected to MongoDB successfully.")

# Load data from MongoDB
data = list(collection.find({}, {"_id": 0}))
df = pd.DataFrame(data)
print(f"Loaded {len(df)} records from 'patient_clusters' collection.")

# Compute mean values per cluster
summary = (
    df.groupby("cluster")[["num_visits", "journey_duration_days", "avg_gap_days"]]
    .mean()
    .reset_index()
)

# Save the summary for reference
summary.to_csv("results/cluster_timeline_summary.csv", index=False)
print("Cluster timeline summary saved to: results/cluster_timeline_summary.csv")

# Plot: Average Journey Duration per Cluster
plt.figure(figsize=(8, 5))
plt.bar(summary["cluster"], summary["journey_duration_days"], color="steelblue", edgecolor="black")
plt.xlabel("Cluster")
plt.ylabel("Average Journey Duration (days)")
plt.title("Average Patient Journey Duration per Cluster")
plt.xticks(summary["cluster"])
plt.grid(axis="y", linestyle="--", alpha=0.7)
plt.tight_layout()

# Save figure
plt.savefig("results/cluster_timeline.png", dpi=300)
print("Cluster timeline plot saved to: results/cluster_timeline.png")

# Close MongoDB connection
client.close()
print("MongoDB connection closed.")

