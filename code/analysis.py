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
		# print(task_logs)
		data = fp.readline()
		job_logs = json.loads(data)
		algorithm = fileName.split('_')[1].split(".")[0].upper()
		t_logs = {'algorithm':[],'task_id': [], 'worker':[], 'time_taken': [], 'start_time':[], 'end_time':[]}
		for key,value in task_logs.items():
			t_logs['algorithm'].append(algorithm)
			t_logs['task_id'].append(key)
			t_logs['time_taken'].append(value[0])
			t_logs['worker'].append(value[1])
			t_logs['start_time'].append(round(value[2][0],2))
			t_logs['end_time'].append(round(value[2][1],2))

		return (t_logs, job_logs)

def calcMetricsTask(task_logs):
	print("===========================================================================\n")
	print('Mean time taken for task completion:', np.mean(task_logs['time_taken']), 'seconds\n')
	print('Median time taken for task completion:',np.median(task_logs['time_taken']), 'seconds\n')
	print("===========================================================================\n")
	
def calcMetricsJob(j_logs):
	print("===========================================================================\n")
	print('Mean time taken for job completion:', np.mean(list(j_logs.values())), 'seconds\n')
	print('Median time taken for job completion:',np.median(list(j_logs.values())), 'seconds\n')
	print("===========================================================================\n")

def plot_bar(df,x,y, algo):
	sns.set(style='whitegrid', font_scale=0.5)
	fig, ax = plt.subplots(figsize=(4.5,4.5))
	sns.barplot(ax=ax,
		data=df,
		x=x, y=y,palette='deep')
	ax.set_ylabel(y)
	# ax.legend()
	ax.set_xlabel(x)
	plt.title(x + " vs " + y + ' - ' + algo)
	# ax.set_xlim(0,1200)
	filename = "Graphs/" + algo + '_' + y + "_" + x + ".png"
	plt.savefig(filename)
	plt.show()

def metrics_graph(logs):
	mean_logs = logs.groupby('worker').agg(mean_time=('time_taken','mean')).reset_index()
	# mean_logs.reset_index()
	median_logs = logs.groupby('worker').agg(median_time=('time_taken','median')).reset_index()

	print(f"Metrics for {algorithm}\n")
	calcMetricsTask(logs)
	calcMetricsJob(j_logs)

	# print(mean_logs.head())
	plot_bar(mean_logs, 'worker','mean_time', algorithm)
	plot_bar(median_logs, 'worker', 'median_time', algorithm)
	# getLogs(fileName)

logs, j_logs = getLogs(fileName)
logs = pd.DataFrame(logs,columns=['algorithm','task_id','worker','time_taken','start_time','end_time'])
logs = logs.sort_values(by=['worker','start_time'])
algorithm = fileName.split('_')[1].split('.')[0].upper()

metrics_graph(logs)

num_workers = len(logs.worker.unique())
for k in range(1,num_workers+1):
	logs_i = logs[logs['worker']==k]

	t0 = int(min(logs_i['start_time']))
	tn = int(max(logs_i['end_time'])) + 1

	execution = {}
	for i in range(t0,tn):
		count = 0
		for j in range(0, len(logs_i)):
			if logs_i.iloc[j,-2] <= i and logs_i.iloc[j,-1] >= i:
				count+=1

		execution[i] = count

	# print(list(execution.values()))
	worker = "Worker " + str(k)
	plt.step(x=list(execution.keys()), y=list(execution.values()), label=worker)
	co = "C" + str(k-1) + "o"
	# print(co)
	plt.plot(list(execution.keys()),list(execution.values()), co, alpha=0.5)

plt.title(f"Number of Tasks vs. Time: {algorithm}")
plt.xlabel("Timestamp")
plt.ylabel("Number of Tasks")
plt.legend(title='Workers')
file = "Graphs/" + algorithm + "_tasksvstime.png"
plt.savefig(file)
plt.show()
