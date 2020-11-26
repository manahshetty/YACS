import socket
import sys
import json
import threading
import time

# Connect all sockets
# 5001 - Send updates
# 4000 - Receive tasks

def execution():
	exit_counter = 10      # So that it breaks from loop after 10s of no activity.
				# TO BE REMOVED LATER!!!
	while(exit_counter):
		#print(threading.current_thread().name, ": Entered Execution with ", e_pool)
		#time.sleep(2)
		for i in e_pool:
			print(threading.current_thread().name, ": Task: ", i[1]['task_id'])
			if(time.time() >= i[1]['end_time']):
				w_id = i[0]
				print(threading.current_thread().name, ": Removing Task: ", i[1]['task_id'])
				e_pool.remove(i)
				print(threading.current_thread().name, ": Calling update")
				sendUpdate(w_id)					# Notify master of completion
		time.sleep(1)
		if(len(e_pool)==0):
			exit_counter -= 1

def sendUpdate(w_id):
	w_id = str(w_id)
	jUSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	jUSocket.connect(("localhost", 5001))
	jUSocket.send(w_id.encode())
	print("Sent update\n")
	jUSocket.close()
	
def worker1(port, w_id):
	global e_pool
	while(1):
		taskLaunchSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		print("Trying to connect to 4000")
		taskLaunchSocket.connect(("localhost", port))
		print("Connected to 4000")
		r = taskLaunchSocket.recv(1024)					# Read task
		req = ""
		while r:
			req += r.decode()
			r = taskLaunchSocket.recv(1024)
		print(threading.current_thread().name, ": Request = ", req, "\n\n")
		if(req):
			task = json.loads(req)
			print("\t\tWorker ", w_id, ": ", task['task_id'], "\n")
			
			# Add request to the executing pool
			task['end_time'] = time.time() + task['duration']
			e_pool.append([w_id, task])
		
		else:
			break
		taskLaunchSocket.close()
	print("Stop Launching tasks")


e_pool = []

with open(sys.argv[1]) as f:
	config = json.load(f)

# Create threads for each worker
t1 = threading.Thread(target = worker1, args = (config['workers'][0]['port'],config['workers'][0]['worker_id']))
t2 = threading.Thread(target = worker1, args = (config['workers'][1]['port'],config['workers'][1]['worker_id']))
t3 = threading.Thread(target = worker1, args = (config['workers'][2]['port'],config['workers'][2]['worker_id']))

# Create a thread to execute the tasks
t4 = threading.Thread(target = execution, name = "Thread 4")

t1.start()
t2.start()
t3.start()

t4.start()

t1.join()
t2.join()
t3.join()

t4.join()
