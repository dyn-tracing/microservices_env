import csv

bytes_per_csv = []
messed_up = [22, 30]
for i in range(1, 145):
    if i not in messed_up:
        with open(str(i)+'alibaba.txt', 'r') as alibaba_file:
            contents = alibaba_file.read()
            location_total_traces = contents.find("Total bytes: ") + len("Total bytes:  ")
            end_total_traces = contents.find("\n", location_total_traces)
            total_bytes = int(contents[location_total_traces:end_total_traces])
            bytes_per_csv.append(total_bytes)

for i in range(len(messed_up)):
    with open(str(messed_up[i])+'alibaba.txt', 'r') as alibaba_file:
        contents = alibaba_file.read()
        location_total_traces = contents.find("Total bytes: ") + len("Total bytes:  ")
        end_total_traces = contents.find("\n", location_total_traces)
        total_bytes = int(contents[location_total_traces:end_total_traces])
        bytes_per_csv.append(total_bytes)

accumulated_bytes = []
for i in range(len(bytes_per_csv)):
    sum = 0
    for j in range(i):
        sum += bytes_per_csv[j]
    accumulated_bytes.append(sum)
print(accumulated_bytes)

with open("bytes_count.csv", "w") as traces_file:
    writer = csv.writer(traces_file)
    writer.writerow(accumulated_bytes)



