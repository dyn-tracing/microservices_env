#!/bin/env python3
import matplotlib.pyplot as plt


tests = [10, 30, 50, 70, 90, 110, 200, 400]

one_retrieval_averages = [35.21, 32.32,  34.50, 33.57,  33.85, 33.10, 30.23, 31.44]
one_retrieval_medians = [34.48, 31.59, 33.06, 33.21, 32.63, 31.99, 29.90, 30.06]


plt.plot(tests, one_retrieval_averages, label="Average: Single object retrieval")
plt.plot(tests, one_retrieval_medians, label="Median: Single object retrieval")
plt.xlabel('Number of traces generated per second, in thousands')
plt.ylabel('Latency (ms)')

ax = plt.gca()
ax.set_ylim([0, 50])
plt.xticks(tests)
plt.title('Load Test Results')
plt.legend()
plt.savefig("load_test")
