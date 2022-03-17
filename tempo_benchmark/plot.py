import sys
from turtle import color
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

fn = sys.argv[1]
df = pd.read_csv(fn)
df.plot(x="BenchName", y="Median", kind="bar", color="blue")
plt.tight_layout()
plt.ylabel("Time (ms) ")
plt.show()