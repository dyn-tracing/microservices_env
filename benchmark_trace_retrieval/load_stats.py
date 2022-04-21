#!/bin/env python3
import matplotlib.pyplot as plt


tests = [10, 30, 50, 70, 90, 110, 200, 400]

one_retrieval_averages = [35.21, 32.32,  34.50, 33.57,  33.85, 33.10, 30.23, 31.44]
one_retrieval_medians = [34.48, 31.59, 33.06, 33.21, 32.63, 31.99, 29.90, 30.06]
parallel_retrieval_averages = [61.95, 58.87, 55.43, 54.19, 54.97, 54.05, 58.77, 59.78]
parallel_retrieval_medians = [54.65, 49.13, 51.61, 49.41, 51.29, 48.77, 56.08, 56.65]


plt.plot(tests, one_retrieval_averages, label="Average: Single object retrieval")
plt.plot(tests, one_retrieval_medians, label="Median: Single object retrieval")
plt.plot(tests, parallel_retrieval_averages, label="Average: Parallel object retrieval")
plt.plot(tests, parallel_retrieval_medians, label="Median: Parallel object retrieval")
plt.xlabel('Number of traces generated per second, in thousands')
plt.ylabel('Latency (ms)')

ax = plt.gca()
ax.set_ylim([0, 80])
plt.xticks(tests)
plt.title('Load Test Results')
plt.legend()
plt.savefig("load_test")
