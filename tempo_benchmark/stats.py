import numpy as np
import scipy.stats as st
import sys

fn = sys.argv[1]
get_raw_csv = 0
if len(sys.argv) >= 3:
 get_raw_csv = sys.argv[2]

with open(fn) as f:
    lines = f.read().split("\n")
    data_arrays = {}
    for line in lines:
        s = line.split("\t")
        if len(s) == 3 and "ok" not in line and "FAIL" not in line: # ignore meta info
            exp_name = s[0].strip()
            time = s[2]
            time = time.replace("ns/op", "")
            time = float(time.strip()) # want milliseconds, not nanoseconds
            if exp_name in data_arrays.keys():
                data_arrays[exp_name].append(time)
            else:
                data_arrays[exp_name] = [time]

if get_raw_csv == "1":
    print("BenchName,Average,Median,CI,CIoverAVG")

for exp in data_arrays.keys():
        data = np.array(data_arrays[exp])
        avg = "{:.2f}".format(np.average(data)/1000000)
        median = "{:.2f}".format(np.median(data)/1000000)
        ci =  "{:.2f}".format((2*np.std(data)/pow(len(data), 0.5))/1000000)
        ci_div_avg = ((2*np.std(data)/pow(len(data), 0.5))/1000000)/(np.average(data)/1000000)
        if get_raw_csv != "1":
            print(exp, "\n")
            print("\taverage: ", avg, "\tmedian: ", median, "\tconfidence interval", ci, "\t ci/avg", ci_div_avg, "\n")
            #print(exp, "\t\t", avg, "\t", median, "\t", stdev, "%\n")
        if get_raw_csv == "1":
            print(f"{exp.replace('BenchmarkPut', '').replace('-2', '')},{avg},{median},{ci},{ci_div_avg}")