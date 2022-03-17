import sys
from turtle import color
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

fn = sys.argv[1]
df = pd.read_csv(fn)

fig, ax = plt.subplots()

ax.barh(
    df["BenchName"], 
    df["Median"], 
    color='blue', 
    height=0.3,
    xerr=df["CI"],
    capsize=5
)

for i, v in enumerate(df["Median"]):
    ax.text(v - 5, i + .25, "m: " + str(v),
            color = 'red')
    ax.text(v + 3, i + .25, "ci:" + str(df["CI"][i]))

plt.tight_layout()
plt.ylabel("Time (ms) ")
plt.show()