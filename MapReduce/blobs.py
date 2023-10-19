import pandas as pd
from sklearn.datasets import make_blobs

import sys

points = sys.argv[1]
features = sys.argv[2]
clusters = sys.argv[3]
filename = sys.argv[4]

print(f"Points: {points} | Features: {features} | Clusters: {clusters} | @ {filename}")

# Set the number of samples and features
n_samples = int(points)
n_features = int(features)
n_clusters = int(clusters)
cluster_std = 1.0

# Generate the dataset using make_blobs
ff, y = make_blobs(n_samples = n_samples,
                   n_features = n_features,
                   centers = n_clusters,
                   cluster_std = cluster_std)
columns = []
# Create a DataFrame to store the data

for i in range(0,n_features):
    columns.append(f"feature{i}")

data_df = pd.DataFrame(ff, columns = columns)

# Save the data to a CSV filedata_df.to_csv("kmeans_data.csv", index=False)

data_df.to_csv(f"./{filename}", index = False)