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

logs, j_logs = getLogs(fileName)
logs = pd.DataFrame(logs,columns=['algorithm','job_id','task_id','time_taken','worker'])
algorithm = fileName.split('_')[1].split('.')[0].upper()

mean_logs = logs.groupby('worker').agg(mean_time=('time_taken','mean')).reset_index()
# mean_logs.reset_index()
median_logs = logs.groupby('worker').agg(median_time=('time_taken','median')).reset_index()

print(f"Metrics for {algorithm}\n")
calcMetricsTask(logs)
calcMetricsJob(j_logs)

print(mean_logs.head())
plot_bar(mean_logs, 'worker','mean_time', algorithm)
plot_bar(median_logs, 'worker', 'median_time', algorithm)


# For the HeatMaps

print()
ans = input("Plot heatmaps? [Y/N] (Make sure all log files are present)")

if ans.upper()=="Y":
	files = ['logs_random.txt','logs_roundRobin.txt','logs_leastLoaded.txt']
	logs = []
	try: 
		for i in files:
			logs.append(getLogs(i))
	except:
		print("Not all log files are present. Please check and run this python file again.\n")
		exit(0)
	
	new_logs = pd.DataFrame(logs[0][0],columns=['algorithm','job_id','task_id','time_taken','worker'])
	new_logs["R_time"] = logs[0][0]['time_taken']
	new_logs['RR_time'] = logs[1][0]['time_taken']
	new_logs['LL_time'] = logs[2][0]['time_taken']

	new_logs = new_logs[['worker', 'task_id', 'R_time', 'RR_time','LL_time']]
	mean_time = new_logs.groupby(['worker']).mean().reset_index()
	# print(mean_time.head())

	sns.heatmap(data=mean_time.iloc[:,1:], xticklabels=[1,2,3], yticklabels=False)
	plt.xlabel("Workers")
	plt.ylabel('Random \t Round Robin \t Least Loaded')
	plt.title("Color signifies the mean time")
	plt.savefig("Graphs/meantime_worker.png")
	plt.show()

	new_log = new_logs.groupby(['worker']).agg(r_tasks=('task_id','count')).reset_index()
	rr_logs = pd.DataFrame(logs[1][0],columns=['algorithm','job_id','task_id','time_taken','worker']).groupby(['worker']).agg(rr_tasks=('task_id','count')).reset_index()
	ll_logs = pd.DataFrame(logs[2][0],columns=['algorithm','job_id','task_id','time_taken','worker']).groupby(['worker']).agg(ll_tasks=('task_id','count')).reset_index()

	new_log['rr_tasks'] = rr_logs['rr_tasks']
	new_log['ll_tasks'] = ll_logs['ll_tasks']

	sns.heatmap(data=new_log.iloc[:,1:],xticklabels=[1,2,3], yticklabels=False)
	plt.xlabel("Workers")
	plt.ylabel('Random \t Round Robin \t Least Loaded')
	plt.title("Color signifies the number of tasks")
	plt.savefig("Graphs/numtasks_worker.png")
	plt.show()

else:
	print("\n\nProgram Terminated")
	exit(0)