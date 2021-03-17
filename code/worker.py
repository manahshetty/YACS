import socket
import sys
import json
import threading
import time
import trace

def execution():
	while(True):
		for i in e_pool:
			cur_time = time.time()				# Check every task in e_pool
			if(cur_time >= i[1]['end_time']):		# If current_time > end_time set earlier, task complete
				i[1]['end_time'] = cur_time
				w_id = i[0]
				task_id = i[1]['task_id']
				job_id = i[1]['job_id']
				job_type = i[1]['job_type']
				
				e_lock.acquire()
				e_pool.remove(i)			# Remove task from e_pool
				e_lock.release()
				
				print("Completed Task: ", task_id)
				sendUpdate(job_id, task_id, w_id, job_type, i[1]['start_time'], i[1]['end_time'])	# Send update to Master
		#time.sleep(1)

def sendUpdate(job_id, task_id, w_id, job_type, start_time, end_time):

	info = {'job_id': job_id, 'job_type': job_type, 'task_id': task_id, 'w_id': w_id, 'start_time': start_time, 'end_time': end_time}	# Structure message to master
	
	jUSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)	# Connect to Master
	jUSocket.connect(("localhost", 5001))
	message = json.dumps(info)
	jUSocket.send(message.encode())
	jUSocket.close()
	
def worker1(port, w_id):
	global e_pool
	while(1):
		try:
			taskLaunchSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			taskLaunchSocket.connect(("localhost", port))
			r = taskLaunchSocket.recv(1024)					# Read task
		except:
			break
		req = ""
		while r:
			req += r.decode()
			r = taskLaunchSocket.recv(1024)
		if(req):
			request = json.loads(req)
			print("Received Task : ", request['task_id'])
			request['start_time'] = time.time()
			request['end_time'] = request['start_time'] + request['duration']	# Add task completion time to request dict
			
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
t2 = threading.Thread(target = execution, name = "Thread 2", daemon = True)		# Checks tasks in e_pool for completion,
											# Sends updates to master on completion.

t1.start()
t2.start()

t1.join()
t2.killed = True
