import pandas as pd
from sklearn.cluster import KMeans
import numpy as np
import time
import sys

points = sys.argv[1]
features = sys.argv[2]
clusters = sys.argv[3]
filename = sys.argv[4]

ds = pd.read_csv(f"./{filename}")

F = int(features)
K = int(clusters)

old = []
new = []

initial = open("./initial.txt")
final = open("./final.txt")

initsplits = initial.read().split("\n")
for i in range(len(initsplits)-1):
    for j in initsplits[i].split(","):
        old.append(float(j))

finsplits = final.read().split("\n")
for i in range(len(finsplits)-1):
    for j in finsplits[i].split(","):
        new.append(float(j))
    
oldcenters = np.array(old).reshape(K,F)
newcenters = np.array(new).reshape(K,F)

print(oldcenters)

start = time.time()
km = KMeans(n_clusters = K,init = oldcenters).fit(ds)
end = time.time()

for i in range(0,K):
    print(f"HADOOP {newcenters[i]}\nSKLEARN {km.cluster_centers_[i]}\n")
print("--------------------")
info = open("./info.txt")
print(f"Total Python time -> {end - start} s")
print(info.read())