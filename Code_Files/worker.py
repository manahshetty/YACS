import socket
import sys
import json
import threading
import time

def execution():
	exit_counter = 10      # So that it breaks from loop after 10s of no activity.
				# TO BE REMOVED LATER!!!
	while(exit_counter):
		for i in e_pool:				# Check every task in e_pool
			if(time.time() >= i[1]['end_time']):	# If current_time > end_time set earlier, task complete
				w_id = i[0]
				task_id = i[1]['task_id']
				job_id = i[1]['job_id']
				job_type = i[1]['job_type']
				
				e_lock.acquire()
				e_pool.remove(i)		# Remove task from e_pool
				e_lock.release()
				
				sendUpdate(job_id, task_id, w_id, job_type)	# Send update to Master
		time.sleep(1)
		if(len(e_pool)==0):
			exit_counter -= 1

def sendUpdate(job_id, task_id, w_id, job_type):

	info = {'job_id': job_id, 'job_type': job_type, 'task_id': task_id, 'w_id': w_id}	# Structure message to master
	
	jUSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)				# Connect to Master
	jUSocket.connect(("localhost", 5001))
	message = json.dumps(info)
	jUSocket.send(message.encode())
	jUSocket.close()
	
def worker1(port, w_id):
	global e_pool
	while(1):
		taskLaunchSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		taskLaunchSocket.connect(("localhost", port))
		r = taskLaunchSocket.recv(1024)					# Read task
		req = ""
		while r:
			req += r.decode()
			r = taskLaunchSocket.recv(1024)
		if(req):
			request = json.loads(req)
			
			request['end_time'] = time.time() + request['duration']	# Add task completion time to request dict
			e_lock.acquire()
			e_pool.append([w_id, request])				# Add request to the executing pool
			e_lock.release()
		else:
			break
		taskLaunchSocket.close()

# Maintain a pool of executing tasks
# [<w_id>, {'task_id': x, 'job_id' : x, 'job_type': x, 'duration': x, 'end_time': x} ]
e_pool = []				
e_lock = threading.Lock()
	
t1 = threading.Thread(target = worker1, args = (int(sys.argv[1]),int(sys.argv[2])))	# Listens to tasks and adds to e_pool
t2 = threading.Thread(target = execution, name = "Thread 2")				# Checks tasks in e_pool for completion,
											# Sends updates to master on completion.

t1.start()
t2.start()

t1.join()
t2.join()
