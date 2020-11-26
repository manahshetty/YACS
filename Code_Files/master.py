import sys
import json
import socket
import time
import random
import numpy as np
import requests
import threading

# Open all sockets:
# 5000 - Listen to Job requests 				-> jRSocket
# 5001 - Listen to Job updates				-> jUSocket
# 4000, 4001, 4002 - Send task to Worker 1, 2, 3		-> taskLaunchSocket

jRSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
jRSocket.bind(("localhost", 5000))
jRSocket.listen(1)

jUSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
jUSocket.bind(("localhost", 5001))
jUSocket.listen(3)


taskLaunchSocket1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
taskLaunchSocket1.bind(("localhost", 4000))
taskLaunchSocket1.listen(1)

taskLaunchSocket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
taskLaunchSocket2.bind(("localhost", 4001))
taskLaunchSocket2.listen(1)

taskLaunchSocket3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
taskLaunchSocket3.bind(("localhost", 4002))
taskLaunchSocket3.listen(1)

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

def random(request):
	print("I schedule at random.\n")
	
	# Needs to look at config of a worker, pick a machine, decrement slot, send task to w_id.
	for m_task in request['map_tasks']:
	
		#print(threading.current_thread().name, ": Task ", m_task['task_id'])
		w_id = np.random.randint(0,3)
		while(config[w_id][1]==0):
			continue
		if(count):
			break
			
		# Found a w_id with free slot
		print(threading.current_thread().name, ": Sending to Worker ", w_id)
		config[w_id][1]-=1
		print("\t", threading.current_thread().name, "--> Decremented Config: ", config)
		if(w_id == 0):
			conn, addr = taskLaunchSocket1.accept()
		if(w_id == 1):
			conn, addr = taskLaunchSocket2.accept()
		if(w_id == 2):
			conn, addr = taskLaunchSocket3.accept()
		
		# Send job request to w_id
		message = json.dumps(m_task)
		conn.send(message.encode())
		conn.close()
	
		
config = initConfig()
print(config)

# Thread 1 addresses Job Requests
def addressRequests():
	while(1):
		try:
			conn, addr = jRSocket.accept()
		except:
			break
		r = conn.recv(1024)
		req = ""
		while r:
			req += r.decode()
			r = conn.recv(1024)
		request = json.loads(req)
		conn.close()
		print(threading.current_thread().name, ": Received Job Request")
		random(request)
	print(threading.current_thread().name, ": Stopped Reading Requests")

# Thread2 listens to task completion messages from workers and updates the free slots in config.
def updateSlots():
	while(1):
		try:
			conn,addr = jUSocket.accept()
		except:
			break
		w_id = conn.recv(1024).decode()
		while(len(w_id)==0):
			w_id = conn.recv(1024).decode()
		print(threading.current_thread().name, ": Received update from: ", w_id)
		config[int(w_id)-1][1]+=1
		print("\t", threading.current_thread().name, "--> Updated Config: ", config)
		conn.close()
		
		
t1 = threading.Thread(target = addressRequests, name = "Thread1")
t2 = threading.Thread(target = updateSlots, name = "Thread2")
t1.start()
t2.start()
t1.join()
t2.join()
print(config)
jRSocket.close()
jUSocket.close()
taskLaunchSocket.close()
