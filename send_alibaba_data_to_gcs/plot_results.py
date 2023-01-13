import matplotlib.pyplot as plt
import numpy as np

# make data
x_duration = [15017, 22900.5, 30051.5, 31715.5]
x_fanout = [14983.5, 21466, 25558.5, 31498]
x_one_other_call = [27362.5, 32897, 36980, 41913]
x_height = [15328, 21549.5, 26081.5, 31436]
y = [105282, 212114 , 318048, 423719]

new_x_duration = [x/1000 for x in x_duration]
new_x_fanout = [x/1000 for x in x_fanout]
new_x_one_other_call = [x/1000 for x in x_one_other_call]
new_x_height = [x/1000 for x in x_height]

new_trace_nums = [x/1000 for x in y]
# plot
fig, ax = plt.subplots()

plt.plot(new_trace_nums, new_x_duration, label = "duration", linestyle='--', marker='o', color='b')
plt.plot(new_trace_nums, new_x_fanout, label = "fanout", linestyle='--', marker='o', color='r')
plt.plot(new_trace_nums, new_x_one_other_call, label = "one other call", linestyle='--', marker='o', color='g')
plt.plot(new_trace_nums, new_x_height, label = "height", linestyle='--', marker='o', color='c')
plt.title("AliBaba Query Latencies")
plt.ylabel("Latency (s)")
plt.xlabel("Number of AliBaba Traces (in thousands)")
plt.ylim(0, 60)
plt.legend()
plt.show()
plt.savefig('graph.png')
