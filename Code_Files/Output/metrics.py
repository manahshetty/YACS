import json
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

allFiles = 0

if(sys.argv[1] == 'RANDOM'):
	fileName = "logs_random.txt"
elif(sys.argv[1] == 'RR'):
	fileName = "logs_roundRobin.txt"
elif sys.argv[1] == "LL":
	fileName = "logs_leastLoaded.txt"
elif sys.argv[1] == "ALL": # done for graph purposes
	fileNames = ["logs_random.txt","logs_roundRobin.txt","logs_leastLoaded.txt"]
	allFiles = 1
else:
	print("The system argument passed is incorrect. \nPlease run the program again with one of the following options: RANDOM/RR/LL/ALL")
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
			job = key.split("_")[0]
			t_logs['algorithm'].append(algorithm)
			t_logs["job_id"].append(job)
			t_logs['task_id'].append(key)
			t_logs['time_taken'].append(value[0])
			t_logs['worker'].append(value[1])

		return t_logs

def calcMetrics(task_logs):
	print("===========================================================================\n")
	print('Mean time taken for task completion:', np.mean(task_logs['time_taken']), '\n')
	print('Median time taken for task completion:',np.median(task_logs['time_taken']), '\n')
	print("===========================================================================\n")

def plot(df):
	sns.set(style='whitegrid', font_scale=0.5)
	fig, ax = plt.subplots(figsize=(4.5,4.5))
	sns.barplot(ax=ax,
		data=df,
		x="worker", y="number_of_tasks", hue="algorithm", palette='deep')
	ax.set_ylabel("Number of Tasks")
	ax.legend()
	ax.set_xlabel("Worker")
	plt.title("Number of Tasks per Worker for each algorithm")
	# ax.set_xlim(0,1200)
	plt.show()


if allFiles == 1:
	logs = []
	for i in fileNames:
		logs.append(getLogs(i))
	
	logs_random = pd.DataFrame(logs[0], index=None, columns=['algorithm','job_id','task_id','time_taken','worker'])
	logs_rr = pd.DataFrame(logs[1], index=None, columns=['algorithm','job_id','task_id','time_taken','worker'])
	logs_ll = pd.DataFrame(logs[2], index=None, columns=['algorithm','job_id','task_id','time_taken','worker'])

	tasklogs_df = logs_random.append(logs_rr)
	tasklogs_df = tasklogs_df.append(logs_ll)

	tasklogs_df = tasklogs_df[['worker','algorithm','task_id']]
	grouped = tasklogs_df.groupby(['worker','algorithm']).agg(number_of_tasks=('task_id','count'))
	grouped = grouped.reset_index()

	plot(grouped)

else:
	logs = getLogs(fileName)
	algorithm = fileName.split('_')[1].split('.')[0].upper()

	print(f"Metrics for {algorithm}\n")
	calcMetrics(logs)
	
