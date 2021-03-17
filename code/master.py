import sys
import json
import socket
import time
import random
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import requests
import threading
import copy
import trace

def initConfig():
	with open(sys.argv[1]) as f:
		conf = json.load(f)

	config = conf['workers']
	
	worker_id_to_index = {}					# dict. { <worker_id> : <index into config>,...}
	for i in range(len(config)):
		worker_id_to_index[config[i]['worker_id']] = i
		
	return config, worker_id_to_index
	
def launchTask(w_id, job_id, job_type, task):

	config_lock.acquire()
	config[w_id]['slots']-=1					# Decrement the number of free slots
	config_lock.release()
	
	c = [{'worker_id' : i['worker_id'] , 'slots' : i['slots']} for i in config]
	print("STATUS: ", c, "\n\n")
	
	if(w_id == 0):							# Choose socket/port based on Worker
		conn, addr = taskLaunchSocket1.accept()
	if(w_id == 1):
		conn, addr = taskLaunchSocket2.accept()
	if(w_id == 2):
		conn, addr = taskLaunchSocket3.accept()

	task['job_id'] = job_id					# Add job_id and job_type (M or R) to message to be sent
	task['job_type'] = job_type
	
	task_logs_lock.acquire()
	task_logs[task['task_id']] = [0, config[w_id]['worker_id']]	# Add task start time to log
	task_logs_lock.release()
	message = json.dumps(task)							# Send task to Worker
	conn.send(message.encode())
	conn.close()

def random(job_id, tasks, job_type):

	for task in tasks:
		config_lock.acquire()						
		w_id = np.random.randint(0,3)				
		while(config[w_id]['slots']==0):				# While randomly picked worker has no free slots
			config_lock.release()
			time.sleep(1)
			config_lock.acquire()
			w_id = np.random.randint(0,3)				# Randomly pick another
		
		print(task['task_id'], " allotted to Worker: ", config[w_id]['worker_id'])
		
		config_lock.release()
		launchTask(w_id, job_id, job_type, task)			# Initiate send task to Worker
		

def roundRobin(job_id, tasks, job_type):

	for task in tasks:
		config_lock.acquire()
		config2 = copy.deepcopy(config)
		config2.sort(key = lambda x: x['worker_id'])
		
		w_id = 0
		while(config2[w_id]['slots']==0):				# While current worker has no free slots
			config_lock.release()
			time.sleep(1)
			
			w_id = (w_id+1)%3					# pick the next
			
			config_lock.acquire()
			config2 = copy.deepcopy(config)
			config2.sort(key = lambda x: x['worker_id'])
			
		print(task['task_id'], " allotted to Worker: ", config[w_id]['worker_id'])
		
		config_lock.release()

		launchTask(w_id, job_id, job_type, task)
		
def leastLoaded(job_id, tasks, job_type):

	for task in tasks:
		config_lock.acquire()
		config2 = copy.deepcopy(config)					
		config2.sort(key=lambda x: x['slots'], reverse=True)		# Sort a copy of config based on free slots > desc
		while(config2[0]['slots']==0):				# If no worker has a free slot, wait 1s and try again
			config_lock.release()
			time.sleep(1)						# If no slots are free, wait for 1s
			config_lock.acquire()
			config2 = copy.deepcopy(config)
			config2.sort(key=lambda x: x['slots'], reverse=True)
		
		w_id = worker_id_to_index[config2[0]['worker_id']]		# w_id = machine with most free slots | Get index
		print(task['task_id'], " allotted to Worker: ", config[w_id]['worker_id'])
		
		config_lock.release()
		
		launchTask(w_id, job_id, job_type, task)			# Initiate send task to worker


def pickScheduler(job_id, tasks, job_type):					# Calls scheduling algo based on CL arg
	if(sys.argv[2] == "RANDOM"):						
		random(job_id, tasks, job_type)
	elif(sys.argv[2] == "RR"):
		roundRobin(job_id, tasks, job_type)
	else:
		leastLoaded(job_id, tasks, job_type)
		
def monitorReduce():
	scheduled = []						# Keep track of reduce tasks that have already been schd.
	
	scheduling_pool_lock.acquire()
	scheduling_pool_copy = copy.deepcopy(scheduling_pool)	# Create a copy so size doesn't change during iter
	scheduling_pool_lock.release()
	
	while(1):
		if(len(scheduling_pool_copy)>0):
			for job_id, status in scheduling_pool_copy.items():
				if(len(status[1]) == 0 and job_id not in scheduled):	# If all m_tasks are complete + not already been schd.
					scheduled.append(job_id)			# Add task to list of schd. tasks
					pickScheduler(job_id, status[0], 'R')		# Pick scheduling algo based on CL arg

		time.sleep(1)	
		
		scheduling_pool_lock.acquire()
		scheduling_pool_copy = copy.deepcopy(scheduling_pool)
		scheduling_pool_lock.release()	
	
# Thread 1 addresses Job Requests
def addressRequests():
	global job_count
	global start_time
	flag = 0
	while(1):
		try:
			conn, addr = jRSocket.accept()
		except:
			break
		r = conn.recv(1024)						# Read job request
		req = ""
		while r:							# If len(req) > 1024b
			req += r.decode()
			r = conn.recv(1024)
		request = json.loads(req)	
		if(flag == 0):
			start_time = time.time()
			flag = 1				
		conn.close()
		
		job_count_lock.acquire()
		job_count += 1
		job_count_lock.release()
		
		job_logs_lock.acquire()
		job_logs[request['job_id']] = time.time()			# Record job start time
		job_logs_lock.release()
		
		scheduling_pool_lock.acquire()				# Add job to scheduling_pool
		scheduling_pool[request['job_id']] = [request['reduce_tasks'], [i['task_id'] for i in request['map_tasks']]]
		scheduling_pool_lock.release()
		
		pickScheduler(request['job_id'], request['map_tasks'], 'M')	# Schedule m_tasks based on algo
	print("\n...")

def updateSlots():
	global job_count
	while(1):
		try:
			conn,addr = jUSocket.accept()
		except:
			break
		u = conn.recv(1024).decode()							# Read task completion info
		update = ""
		while(len(u)!=0):
			update += u
			u = conn.recv(1024).decode()
		update = json.loads(update)
		
		task_logs_lock.acquire()
		task_logs[update['task_id']][0] = update['end_time'] - update['start_time'] 	# Record end time and add to task log
		task_logs[update['task_id']].append([update['start_time'] - start_time, update['end_time'] - start_time])
		task_logs_lock.release()

		w_id = worker_id_to_index[update['w_id']]				# Convert the worker_id to index into config
		
		config_lock.acquire()
		config[w_id]['slots']+=1						# Increment free slots on resp. worker
		config_lock.release()

		
		print(update['task_id'], " completed by Worker: ", config[w_id]['worker_id'])
		c = [{'worker_id' : i['worker_id'] , 'slots' : i['slots']} for i in config]
		print("STATUS: ", c, "\n\n")
	
		
		if(update['job_type'] == 'M'):						# If it was a map task
			
			scheduling_pool_lock.acquire()
			scheduling_pool[update['job_id']][1].remove(update['task_id'])	# Remove from resp job's m_task list
			scheduling_pool_lock.release()
			
		else:										# If it was a reduce task
			for task in scheduling_pool[update['job_id']][0]:
				if task['task_id'] == update['task_id']:
					
					scheduling_pool_lock.acquire()
					scheduling_pool[update['job_id']][0].remove(task)	# Remove from resp job's r_task list
					scheduling_pool_lock.release()
					break
					
			if(len(scheduling_pool[update['job_id']][0]) == 0):			# If no more r_tasks in resp job
												# Job completed
				print("\n", "=" * 105, "\n")
				print("\t\t\t\t *************** COMPLETED JOB ", update['job_id'], "***************")
				print("\n", "=" * 105, "\n")
				
				job_logs_lock.acquire()
				job_logs[update['job_id']] = update['end_time'] - job_logs[update['job_id']]	# Update duration of job
				job_logs_lock.release()
				
				scheduling_pool_lock.acquire()
				scheduling_pool.pop(update['job_id'])				# Remove job from scheduling_pool
				scheduling_pool_lock.release()
				
				job_count_lock.acquire()
				job_count -= 1
				job_count_lock.release()
				
		conn.close()
	print("\n...")

# Initialize Configuration.
config, worker_id_to_index = initConfig()
config_lock = threading.Lock()
print("="*105, "\n")
print("\t"*5, "MASTER INITIALIZED")
print("\t"*4, "     WORKERS CAN BE INITIATED\n")
print("="*105, "\n")
		
print("INITIAL STATUS: ", config, "\n")


# Initialize Sockets
# 5000 - Listen to Job requests
# 5001 - Listen to Job updates
# config[i][2] - Launch Tasks on Worker i

jRSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
jRSocket.settimeout(60.0)
jRSocket.bind(("localhost", 5000))
jRSocket.listen(1)

jUSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
jUSocket.settimeout(100.0)
jUSocket.bind(("localhost", 5001))
jUSocket.listen(3)


taskLaunchSocket1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
taskLaunchSocket1.bind(("localhost", config[0]['port']))
taskLaunchSocket1.listen(1)

taskLaunchSocket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
taskLaunchSocket2.bind(("localhost", config[1]['port']))
taskLaunchSocket2.listen(1)

taskLaunchSocket3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
taskLaunchSocket3.bind(("localhost", config[2]['port']))
taskLaunchSocket3.listen(1)


# Initialize time logs
job_logs = {}			# <job_id> : time
job_logs_lock = threading.Lock()
task_logs = {}			# <task_id> : [time, worker]
task_logs_lock = threading.Lock()

job_count = 0
job_count_lock = threading.Lock()

start_time = 0

# Initialize shared data structure.
# Keeps record of job requests yet to complete exec
# Used to track map task completion. 
#	Removes task from task list on completion.
#	If len(map_tasks) == 0, launch reduce tasks.

scheduling_pool = {}				# {job_id : [ [r_tasks {dict of id and dur}],[m_tasks {list of task ids] ],...}
scheduling_pool_lock = threading.Lock()

t1 = threading.Thread(target = addressRequests, name = "Thread1")	# Listens to Job Requests and schedules
t2 = threading.Thread(target = updateSlots, name = "Thread2")	# Listens to updates on Task Completion
t3 = threading.Thread(target = monitorReduce, name = "Thread3", daemon = True)	# Checks for completion m_tasks to launch r_tasks

t1.start()
t2.start()
t3.start()

t1.join()
t2.join()
t3.killed = True							# Kill t3 so that it doesn't keep running in the bg
									# Main thread executes this only once t1&t2 have terminated. => All jobs complete.


jRSocket.close()
jUSocket.close()
taskLaunchSocket1.close()
taskLaunchSocket2.close()
taskLaunchSocket3.close()


print("\n", "=" * 105, "\n")
print("\nTASK LOGS:\n", task_logs)
print("\nJOB LOGS:\n", job_logs)
print("\n", "=" * 105, "\n")
print("\n\n", "#"*105)
print("<" * 49, " EXIT ", ">" * 49)
print("#" * 105, "\n")

if(sys.argv[2] == 'RANDOM'):
	fileName = "logs_random.txt"
elif(sys.argv[2] == 'RR'):
	fileName = "logs_roundRobin.txt"
else:
	fileName = "logs_leastLoaded.txt"
	
fp = open(fileName, 'w')
fp.write(json.dumps(task_logs))
fp.write('\n')
fp.write(json.dumps(job_logs))
fp.close()
