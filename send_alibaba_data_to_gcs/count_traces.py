import csv

traces_per_csv = []
messed_up = [22, 30]
for i in range(1, 145):
    if i not in messed_up:
        with open(str(i)+'alibaba.txt', 'r') as alibaba_file:
            contents = alibaba_file.read()
            location_total_traces = contents.find("total traces: ") + len("total traces: ")
            end_total_traces = contents.find("\n", location_total_traces)
            print("alibaba file name is ", str(i))
            total_traces = int(contents[location_total_traces:end_total_traces])

            location_cyclic_traces = contents.find("(cyclic): ") + len("(cyclic): ")
            end_cyclic_traces = contents.find("\n", location_cyclic_traces)
            cyclic_traces = int(contents[location_cyclic_traces:end_cyclic_traces])

            location_fragmented_traces = contents.find("(frag): ") + len("(frag): ")
            end_fragmented_traces = contents.find("\n", location_fragmented_traces)
            fragmented_traces = int(contents[location_fragmented_traces:end_fragmented_traces])
            traces_per_csv.append(total_traces-(cyclic_traces+fragmented_traces))

for i in range(len(messed_up)):
    with open(str(messed_up[i])+'alibaba.txt', 'r') as alibaba_file:
        contents = alibaba_file.read()
        location_total_traces = contents.find("total traces: ") + len("total traces: ")
        end_total_traces = contents.find("\n", location_total_traces)
        print("alibaba file name is ", str(messed_up[i]))
        total_traces = int(contents[location_total_traces:end_total_traces])

        location_cyclic_traces = contents.find("(cyclic): ") + len("(cyclic): ")
        end_cyclic_traces = contents.find("\n", location_cyclic_traces)
        cyclic_traces = int(contents[location_cyclic_traces:end_cyclic_traces])

        location_fragmented_traces = contents.find("(frag): ") + len("(frag): ")
        end_fragmented_traces = contents.find("\n", location_fragmented_traces)
        fragmented_traces = int(contents[location_fragmented_traces:end_fragmented_traces])
        traces_per_csv.append(total_traces-(cyclic_traces+fragmented_traces))
accumulated_traces = []
for i in range(len(traces_per_csv)):
    sum = 0
    for j in range(i):
        sum += traces_per_csv[j]
    accumulated_traces.append(sum)
print(accumulated_traces)

with open("traces_count.csv", "w") as traces_file:
    writer = csv.writer(traces_file)
    writer.writerow(accumulated_traces)



