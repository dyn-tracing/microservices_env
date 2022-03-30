import sys
from turtle import color
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

fn = sys.argv[1]
df = pd.read_csv(fn)

fig, ax = plt.subplots()

ax.bar(
    df["BenchName"], 
    df["Median"], 
    color='blue', 
    width=0.3,
    yerr=df["CI"],
    capsize=5
)

for i, v in enumerate(df["Median"]):
    ax.text(i-0.05, v+6, str(v) + u"\u00B1" + str(df["CI"][i]),
            color = 'red', rotation=60)

plt.xticks(rotation = 45)
plt.tight_layout()
plt.ylabel("Time (ms) ")
plt.show()