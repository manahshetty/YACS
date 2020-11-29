import json
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

if(sys.argv[1] == 'RANDOM'):
	fileName = "logs_random.txt"
elif(sys.argv[1] == 'RR'):
	fileName = "logs_roundRobin.txt"
else:
	fileName = "logs_leastLoaded.txt"
	
with open(fileName) as fp:
	data = fp.readline()
	task_logs = json.loads(data)
	data = fp.readline()
	job_logs = json.loads(data)
	
print("\n===========================================================================\n")

t_logs = {'job_id': [],'task_id': [], 'time_taken': [], 'worker':[]}
for key,value in task_logs.items():
	job = key.split("_")[0]
	t_logs["job_id"].append(job)
	t_logs['task_id'].append(key)
	t_logs['time_taken'].append(value[0])
	t_logs['worker'].append(value[1])

print('Mean time taken for task completion:', np.mean(t_logs['time_taken']), '\n')
print('Median time taken for task completion:',np.median(t_logs['time_taken']), '\n')

print("\n===========================================================================\n")


tasklogs_df = pd.DataFrame(t_logs,index=None,columns=['job_id','task_id','time_taken','worker'])

n_tasks_per_worker = sorted(tasklogs_df.worker.value_counts())
timetaken_per_worker = [sum(tasklogs_df[tasklogs_df['worker']==i]['time_taken']) for i in n_tasks_per_worker]
worker = [1,2,3]


# x = np.arange(len(labels))  # the label locations
# width = 0.35  # the width of the bars

# fig, ax = plt.subplots()
# rects1 = ax.bar(x - width/2, men_means, width, label='Men')
# rects2 = ax.bar(x + width/2, women_means, width, label='Women')

# # Add some text for labels, title and custom x-axis tick labels, etc.
# ax.set_ylabel('Scores')
# ax.set_title('Scores by group and gender')
# ax.set_xticks(x)
# ax.set_xticklabels(labels)
# ax.legend()

'''
def autolabel(rects):
    """Attach a text label above each bar in *rects*, displaying its height."""
    for rect in rects:
        height = rect.get_height()
        ax.annotate('{}'.format(height),
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')


autolabel(rects1)
autolabel(rects2)

fig.tight_layout()

plt.show()
'''

