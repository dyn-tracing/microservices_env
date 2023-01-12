
# 6th batch
seconds_per_experiment = 46
number_of_csvs = 10

total = 0
for i in range(number_of_csvs):
    total += 900 # add 15 min to get data in storage

    # seconds_per_experiment is len of one experiment,
    # 4 is number of experiments, 10 is number of repetitions per experiment
    total += seconds_per_experiment * 4 * 10

    # we increase at roughly rate of 6 seconds per csv
    seconds_per_experiment += 6

print("total number of seconds: ", total)
print("which is actually in minutes as: ", total/60.0)
print("which is actually in hours as: ", total/(60.0*60.0))
