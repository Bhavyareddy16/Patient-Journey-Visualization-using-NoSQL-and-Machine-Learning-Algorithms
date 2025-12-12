import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from pymongo import MongoClient
import matplotlib
matplotlib.use("Agg")  # Disable GUI backend (prevents macOS freeze)
import matplotlib.pyplot as plt

# ---------------------------------------------------
# 1. Connect to MongoDB
# ---------------------------------------------------
try:
    client = MongoClient("mongodb://localhost:27017/")
    db = client["patient_journey"]
    print("Connected to MongoDB successfully.")
except Exception as e:
    print("Error connecting to MongoDB:", e)
    exit()

# ---------------------------------------------------
# 2. Load data from 'patient_features' collection
# ---------------------------------------------------
collection = db["patient_features"]
data = list(collection.find({}, {"_id": 0}))
df = pd.DataFrame(data)

if df.empty:
    print("No data found in 'patient_features' collection.")
    exit()

print(f"Loaded {len(df)} records from 'patient_features' collection.")

# ---------------------------------------------------
# 3. Select numeric features for clustering
# ---------------------------------------------------
features = ["num_visits", "journey_duration_days", "avg_gap_days"]
X = df[features]

# ---------------------------------------------------
# 4. Standardize the data
# ---------------------------------------------------
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# ---------------------------------------------------
# 5. Apply K-Means clustering
# ---------------------------------------------------
k = 4  # You can adjust this number if needed
kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
df["cluster"] = kmeans.fit_predict(X_scaled)

sil_score = silhouette_score(X_scaled, df["cluster"])
print(f"Clustering complete. Silhouette Score: {sil_score:.3f}")

# ---------------------------------------------------
# 6. Display cluster centers
# ---------------------------------------------------
centers = pd.DataFrame(
    scaler.inverse_transform(kmeans.cluster_centers_),
    columns=features
)
centers.index.name = "cluster"
print("\nCluster Centers (mean values):")
print(centers)

# ---------------------------------------------------
# 7. Save clustered data back to MongoDB
# ---------------------------------------------------
clustered_data = df.to_dict("records")
db["patient_clusters"].drop()  # Clear old data
db["patient_clusters"].insert_many(clustered_data)
print(f"Saved {len(df)} clustered records into 'patient_clusters' collection.")

# ---------------------------------------------------
# 8. Save visualization (no popup window)
# ---------------------------------------------------
plt.figure(figsize=(8, 6))
plt.scatter(
    df["num_visits"],
    df["journey_duration_days"],
    c=df["cluster"],
    cmap="viridis",
    alpha=0.6
)
plt.xlabel("Number of Visits")
plt.ylabel("Journey Duration (days)")
plt.title("Patient Journey Clusters")
plt.grid(True)
plt.savefig("cluster_visualization.png", dpi=300)
print("Visualization saved as 'cluster_visualization.png'.")

# ---------------------------------------------------
# 9. Close MongoDB connection
# ---------------------------------------------------
client.close()
print("MongoDB connection closed.")

