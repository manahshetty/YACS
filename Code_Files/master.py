import sys
import json
import socket
import time
import random
import numpy as np
import requests
import threading
import copy

def initConfig():
	with open(sys.argv[1]) as f:
		conf = json.load(f)

	# structure: [ [id, slot, port], [id, slot, port],... ]
	config = []
	for k, v in conf.items():
		for worker in v:
			l = []
			l += worker.values()
			config += [l]
	return config
	
def launchTask(w_id, job_id, job_type, task):

	config_lock.acquire()
	config[w_id][1]-=1					# Decrement the number of free slots
	config_lock.release()
	
	if(w_id == 0):						# Choose socket/port based on Worker
		conn, addr = taskLaunchSocket1.accept()
	if(w_id == 1):
		conn, addr = taskLaunchSocket2.accept()
	if(w_id == 2):
		conn, addr = taskLaunchSocket3.accept()

	task['job_id'] = job_id				# Add job_id and job_type (M or R) to message to be sent
	task['job_type'] = job_type
		
	task_logs[task['task_id']] = [time.time(), w_id+1]	# Add task start time to log
	message = json.dumps(task)				# Send task to Worker
	conn.send(message.encode())
	conn.close()

def random(job_id, tasks, job_type):
	#print("I schedule at random.\n")
	for task in tasks:						
		w_id = np.random.randint(0,3)				
		while(config[w_id][1]==0):					# While randomly picked worker has no free slots
			w_id = np.random.randint(0,3)				# Randomly pick another
			
		launchTask(w_id, job_id, job_type, task)			# Initiate send task to Worker
		

def roundRobin(job_id, tasks, job_type):
	#print("I like Round Robin.\n")
	for task in tasks:
		w_id = 0
		while(config[w_id][1]==0):					# While current worker has no free slots
			w_id = (w_id+1)%3					# pick the next
		
		launchTask(w_id, job_id, job_type, task)
		
def leastLoaded(job_id, tasks, job_type):
	#print("I prefer my loads light.\n")
	for task in tasks:
		config2 = copy.copy(config)					
		config2.sort(key=lambda x: x[1], reverse=True)		# Sort a copy of config based on free slots > desc
		while(config2[1]==0):						# If no worker has a free slot, wait 1s and try again
			time.sleep(1)					# If no slots are free, wait for 1s
			config2 = copy.copy(config)
			config2.sort(key=lambda x: x[1], reverse=True)
		
		w_id = config2[0][0] - 1					# w_id = machine with most free slots
		launchTask(w_id, job_id, job_type, task)			# Initiate send task to worker


def pickScheduler(job_id, tasks, job_type):					# Calls scheduling algo based on CL arg
	if(sys.argv[2] == "random"):						
		random(job_id, tasks, job_type)
	elif(sys.argv[2] == "rr"):
		roundRobin(job_id, tasks, job_type)
	else:
		leastLoaded(job_id, tasks, job_type)
		
		
def monitorReduce():
	scheduled = []						# Keep track of reduce tasks that have already been schd.
	while(1):
		if(len(scheduling_pool)>0):
			scheduling_pool_lock.acquire()
			for job_id, status in scheduling_pool.items():
				if(len(status[1]) == 0 and job_id not in scheduled):	# If all m_tasks are complete + not already been schd.
					
					scheduled.append(job_id)			# Add task to list of schd. tasks
					pickScheduler(job_id, status[0], 'R')		# Pick scheduling algo based on CL arg
					
			scheduling_pool_lock.release()
		time.sleep(1)							# Wait for 1s before checking again.
				
	
# Thread 1 addresses Job Requests
def addressRequests():
	global job_count
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
		conn.close()
		
		job_count_lock.acquire()
		job_count += 1
		job_count_lock.release()
		
		job_logs[request['job_id']] = time.time()			# Record job start time
		
		scheduling_pool_lock.acquire()				# Add job to scheduling_pool
		scheduling_pool[request['job_id']] = [request['reduce_tasks'], [i['task_id'] for i in request['map_tasks']]]
		scheduling_pool_lock.release()
		
		pickScheduler(request['job_id'], request['map_tasks'], 'M')	# Schedule m_tasks based on algo
	

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
		
		end_time = time.time()								# Record end time and add to task log
		task_logs[update['task_id']][0] = end_time - task_logs[update['task_id']][0]
		
		config_lock.acquire()
		config[update['w_id']-1][1]+=1						# Increment free slots on resp. worker
		config_lock.release()
		
		if(update['job_type'] == 'M'):						# If it was a map task
			
			scheduling_pool_lock.acquire()
			scheduling_pool[update['job_id']][1].remove(update['task_id'])	# Remove from resp job's m_task list
			scheduling_pool_lock.release()
			'''
			if(len(scheduling_pool[update['job_id']][1]) == 0):
				random(update['job_id'], scheduling_pool[update['job_id']][0], 'R')
			'''
		else:										# If it was a reduce task
			for task in scheduling_pool[update['job_id']][0]:
				if task['task_id'] == update['task_id']:
					
					scheduling_pool_lock.acquire()
					scheduling_pool[update['job_id']][0].remove(task)	# Remove from resp job's r_task list
					scheduling_pool_lock.release()
					break
					
			if(len(scheduling_pool[update['job_id']][0]) == 0):			# If no more r_tasks in resp job
												# Job completed
				print("\n===========================================================================\n")
				print("\t\t\t ******** COMPLETED JOB ", update['job_id'], "*********")
				print("\n===========================================================================\n")
				job_logs[update['job_id']] = end_time - job_logs[update['job_id']]	# Update duration of job
				
				scheduling_pool_lock.acquire()
				scheduling_pool.pop(update['job_id'])				# Remove job from scheduling_pool
				scheduling_pool_lock.release()

				job_count_lock.acquire()
				job_count -= 1
				job_count_lock.release()

				if(len(scheduling_pool)==0):
				#if(job_count == 0):						# Can use any If
					print("\n===========================================================================\n")
					print("\nTASK LOGS:\n", task_logs)
					print("\nJOB LOGS:\n", job_logs)
					print("\n===========================================================================\n")
					print("\n\n############################################################################")
					print("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< EXIT >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
					print("############################################################################\n")
		conn.close()

# Initialize Configuration.
config = initConfig()
config_lock = threading.Lock()		
print(config)

# Initialize Sockets
# 5000 - Listen to Job requests
# 5001 - Listen to Job updates
# config[i][2] - Launch Tasks on Worker i

jRSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
jRSocket.bind(("localhost", 5000))
jRSocket.listen(1)

jUSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
jUSocket.bind(("localhost", 5001))
jUSocket.listen(3)


taskLaunchSocket1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
taskLaunchSocket1.bind(("localhost", config[0][2]))
taskLaunchSocket1.listen(1)

taskLaunchSocket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
taskLaunchSocket2.bind(("localhost", config[1][2]))
taskLaunchSocket2.listen(1)

taskLaunchSocket3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
taskLaunchSocket3.bind(("localhost", config[2][2]))
taskLaunchSocket3.listen(1)

# Initialize time logs
job_logs = {}			# <job_id> : time
task_logs = {}			# <task_id> : [time, worker]

job_count = 0
job_count_lock = threading.Lock()

# Initialize shared data structure.
# Keeps record of job requests yet to complete exec
# Used to track map task completion. 
#	Removes task from task list on completion.
#	If len(map_tasks) == 0, launch reduce tasks.

scheduling_pool = {}				# {job_id : [ [r_tasks {dict of id and dur}],[m_tasks {list of task ids] ],...}
scheduling_pool_lock = threading.Lock()

t1 = threading.Thread(target = addressRequests, name = "Thread1")	# Listens to Job Requests and schedules
t2 = threading.Thread(target = updateSlots, name = "Thread2")	# Listens to updates on Task Completion
t3 = threading.Thread(target = monitorReduce, name = "Thread3")	# Checks for completion m_tasks to launch r_tasks

t1.start()
t2.start()
t3.start()

t1.join()
t2.join()
t3.join()
print(config)
jRSocket.close()
jUSocket.close()
taskLaunchSocket.close()
