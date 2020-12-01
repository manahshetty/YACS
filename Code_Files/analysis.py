import json
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Run: python3 analysis.py <algorithm>

if(sys.argv[1] == 'RANDOM'):
	fileName = "logs_random.txt"
elif(sys.argv[1] == 'RR'):
	fileName = "logs_roundRobin.txt"
elif sys.argv[1] == "LL":
	fileName = "logs_leastLoaded.txt"
else:
	print("The first system argument passed is incorrect. \nPlease run the program again with one of the following options: RANDOM/RR/LL")
	print("Program terminated.")
	exit(0)

def getLogs(fileName):
	with open(fileName) as fp:
		data = fp.readline()
		task_logs = json.loads(data)
		data = fp.readline()
		job_logs = json.loads(data)
		algorithm = fileName.split('_')[1].split(".")[0].upper()
		t_logs = {'algorithm':[],'job_id': [],'task_id': [], 'time_taken': [], 'worker':[]}
		for key,value in task_logs.items():

			# if value[1] == 3:
			# 	print("YAAS")

			job = key.split("_")[0]
			t_logs['algorithm'].append(algorithm)
			t_logs["job_id"].append(job)
			t_logs['task_id'].append(key)
			t_logs['time_taken'].append(value[0])
			t_logs['worker'].append(value[1])

		return t_logs

def calcMetrics(task_logs):
	print("===========================================================================\n")
	print('Mean time taken for task completion:', np.mean(task_logs['time_taken']), 'seconds\n')
	print('Median time taken for task completion:',np.median(task_logs['time_taken']), 'seconds\n')
	print("===========================================================================\n")

def plot_bar(df, att):
	sns.set(style='whitegrid', font_scale=0.5)
	fig, ax = plt.subplots(figsize=(4.5,4.5))
	sns.barplot(ax=ax,
		data=df,
		x="worker", y=att,palette='deep')
	ax.set_ylabel(att)
	# ax.legend()
	ax.set_xlabel("Worker")
	if att == "mean_time":
		plt.title("Mean Time per Worker")
	else:
		plt.title("Median Time per Worker")
	# ax.set_xlim(0,1200)
	filename = att + '_workers.png'
	plt.savefig(filename)
	plt.show()

logs = getLogs(fileName)
logs = pd.DataFrame(logs,columns=['algorithm','job_id','task_id','time_taken','worker'])
algorithm = fileName.split('_')[1].split('.')[0].upper()

mean_logs = logs.groupby('worker').agg(mean_time=('time_taken','mean')).reset_index()
# mean_logs.reset_index()
median_logs = logs.groupby('worker').agg(median_time=('time_taken','median')).reset_index()

print(f"Metrics for {algorithm}\n")
calcMetrics(logs)

# print(mean_logs.head())
plot_bar(mean_logs, 'mean_time')
plot_bar(median_logs, 'median_time')
	
