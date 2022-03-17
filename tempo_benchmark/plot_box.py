import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import sys

fn = sys.argv[1]

with open(fn) as f:
    lines = f.read().split("\n")
    data_arrays = {}
    for line in lines:
        s = line.split("\t")
        if len(s) == 3 and "ok" not in line and "FAIL" not in line: # ignore meta info
            exp_name = s[0].strip()
            time = s[2]
            time = time.replace("ns/op", "")
            time = float(time.strip())/1000000 # want milliseconds, not nanoseconds
            time = float("{:.2f}".format(time))
            if exp_name in data_arrays.keys():
                data_arrays[exp_name].append(time)
            else:
                data_arrays[exp_name] = [time]

data = [
    data_arrays["BenchmarkGetTwoBytes-2"],
    data_arrays["BenchmarkGetHundredBytes-2"],
    data_arrays["BenchmarkGetThousandBytes-2"],
    data_arrays["BenchmarkGetTenThousandBytes-2"],
    data_arrays["BenchmarkGetHundredThousandBytes-2"],
    data_arrays["BenchmarkGetMegaBytes-2"],
    data_arrays["BenchmarkGetBigTraceBytes-2"],
    data_arrays["BenchmarkGetSmallTraceBytes-2"],
    data_arrays["BenchmarkGetTinyTraceBytes-2"]
]

fig = plt.figure(figsize =(10, 7))
ax = fig.add_subplot(111)
 
# Creating axes instance
bp = ax.boxplot(data, patch_artist = True,
                notch ='True', vert = 0)
 
colors = ["blue", "red", "green", "yellow", "black"]
 
for patch, color in zip(bp['boxes'], colors):
    patch.set_facecolor(color)
 
# x-axis labels
ax.set_yticklabels(['2 B', '100 B',
                    '1,000B', '10,000 B', '10,000 B', 'MB', 'BigTrace(49kB)', 'SmallTrace(8kB)', 'TinyTrace(4kB)'])

 
# Adding title
plt.title("GCS Bench")
plt.ylabel("# of bytes")
plt.xlabel("Time(ms)")
     
# show plot
plt.show()