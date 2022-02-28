import numpy as np

with open("out300.txt") as f:
    lines = f.read().split("\n")
    data_arrays = {}
    for line in lines:
        s = line.split("\t")
        if len(s) == 3 and "ok" not in line and "FAIL" not in line: # ignore meta info
            exp_name = s[0]
            time = s[2]
            time = time.replace("ns/op", "")
            time = float(time.strip()) # want milliseconds, not nanoseconds
            if exp_name in data_arrays.keys():
                data_arrays[exp_name].append(time)
            else:
                data_arrays[exp_name] = [time]

for exp in data_arrays.keys():
    data = np.array(data_arrays[exp])
    avg = "{:.2f}".format(np.average(data)/1000000)
    stdev = "{:.2f}".format(np.std(data)*100/np.average(data))
    print(exp, "\t", avg, "\t", stdev, "%\n")
