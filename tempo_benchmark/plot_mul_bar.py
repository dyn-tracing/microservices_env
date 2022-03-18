import numpy as np 
import matplotlib.pyplot as plt 
import sys
import pandas as pd

fn1 = "out-gcs-march-18-2.csv"
df_cold = pd.read_csv(fn1)

fn2 = "out-gcs-march-17-bvm-warm.csv"
df_warm = pd.read_csv(fn2)

X = ['2 B', '100 B', '1,000B', '10,000 B', '100,000 B', 'MB', 'BigTrace(49kB)', 'SmallTrace(8kB)', 'TinyTrace(4kB)']
Cold = df_cold["Median"]
Warm = df_warm["Median"]

X_axis = np.arange(len(X))

plt.barh(X_axis - 0.2, Cold, 0.4, label = 'Cold')
plt.barh(X_axis + 0.2, Warm, 0.4, label = 'Warm')

for i, v in enumerate(df_warm["Median"]):
    plt.text(v - 5, i, str(v) + u"\u00B1" + str(df_warm["CI"][i]),
            color = 'black')
    
    plt.text(df_cold["Median"][i] - 5, i - .35,  str(df_cold["Median"][i]) + u"\u00B1" + str(df_cold["CI"][i]),
            color = 'black')

plt.yticks(X_axis, X)
plt.xlabel("Median Time(ms)")
plt.ylabel("Num of Bytes")
plt.title("GCS Cold vs Warm")
plt.legend()
plt.show()