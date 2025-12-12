# src/analysis/cluster_visualization.py

import os
import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient
from sklearn.decomposition import PCA

# Create results directory if not exists
os.makedirs("results", exist_ok=True)

# Connect to MongoDB
try:
    client = MongoClient("mongodb://localhost:27017/")
    db = client["patient_journey"]
    collection = db["patient_clusters"]
    print("Connected to MongoDB successfully.")
except Exception as e:
    print("Error connecting to MongoDB:", e)
    exit()

# Load data
data = pd.DataFrame(list(collection.find()))
print(f"Loaded {len(data)} clustered patient records.")

# Check required columns
required_cols = ["num_visits", "journey_duration_days", "avg_gap_days", "cluster"]
if not all(col in data.columns for col in required_cols):
    print("Error: One or more required columns missing in 'patient_clusters' collection.")
    exit()

# Reduce to 2D for visualization using PCA
pca = PCA(n_components=2)
features = data[["num_visits", "journey_duration_days", "avg_gap_days"]]
reduced = pca.fit_transform(features)
data["PC1"] = reduced[:, 0]
data["PC2"] = reduced[:, 1]

# Plot clusters
plt.figure(figsize=(10, 7))
for cluster_id in sorted(data["cluster"].unique()):
    cluster_data = data[data["cluster"] == cluster_id]
    plt.scatter(
        cluster_data["PC1"],
        cluster_data["PC2"],
        label=f"Cluster {cluster_id}",
        s=40,
        alpha=0.7
    )

plt.title("Patient Journey Clusters (PCA Visualization)", fontsize=14)
plt.xlabel("Principal Component 1")
plt.ylabel("Principal Component 2")
plt.legend()
plt.grid(True)

# Save figure
output_path = "results/cluster_visualization.png"
plt.savefig(output_path, dpi=300, bbox_inches="tight")
print(f"Cluster visualization saved to: {output_path}")

# Close connection
client.close()
print("MongoDB connection closed.")

