import sys
import json
import socket
import time
import random
import numpy as np
import requests
import threading

# Open all sockets:
# 5000 - Listen to Job requests
# 5001 - Listen to Job updates
# 4000 - Send task to Worker 1

jRSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
jRSocket.bind(("localhost", 5000))
jRSocket.listen(1)

jUSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
jUSocket.bind(("localhost", 5001))
jUSocket.listen(1)


taskLaunchSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
taskLaunchSocket.bind(("localhost", 4000))
taskLaunchSocket.listen(1)

def initConfig():
	with open(sys.argv[1]) as f:
		conf = json.load(f)

	# structure: [ [id, slot, port], [id, slot, port],... ] --> Consider having a field for free slots instead of decrementing slots so as to not indefinitely increment slots.
	config = []
	for k, v in conf.items():
		for worker in v:
			l = []
			l += worker.values()
			config += [l]
	return config

def random(request):
	print("I schedule at random.\n")
	
	print(threading.current_thread().name, ": Connected to Port 4000")
	# Needs to look at config, pick a machine, decrement slot, send request to w_id.
	for m_task in request['map_tasks']:
		print(threading.current_thread().name, ": Task ", m_task['task_id'])
		w_id = 0
		count = 0
		while(config[w_id][1]==0 and count==0):
			count+=1
			continue
		if(count):
			break
		# Found a w_id
		config[w_id][1]-=1
		
		conn, addr = taskLaunchSocket.accept()
		# Send job request to w_id
		print(threading.current_thread().name, ": Connecting to Worker 1")
		print(threading.current_thread().name, ": Connected to Worker 1")
		message = json.dumps(m_task)
		print("Message being sent: ", message)
		conn.send(message.encode())
		print(threading.current_thread().name, ": Sent request to Worker 1")

		conn.close()
	
		
config = initConfig()
print(config)

# Thread 1 addresses Job Requests
def addressRequests():
	while(1):
		try:
			print(threading.current_thread().name, ": Trying to connect to Port 5000")
			conn, addr = jRSocket.accept()
			print(threading.current_thread().name, ": Connected to Port 5000")
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
		print(request, "\n")
		random(request)
	print(threading.current_thread().name, ": Stopped Reading Requests")

def updateSlots():
	#conn,addr = jUSocket.accept()
	while(1):
		try:
			print(threading.current_thread().name, ": Trying to connect to Port 5001")
			conn,addr = jUSocket.accept()
			print(threading.current_thread().name, "Connected to Port 5001")
		except:
			print(threading.current_thread().name, "Could not connect to Port 5001")
			break
		w_id = conn.recv(1024).decode()
		while(len(w_id)==0):
			w_id = conn.recv(1024).decode()
		config[int(w_id)-1][1]+=1
		print(threading.current_thread().name, "Updated Config: ", config)
		print(threading.current_thread().name, ": Received update from: ", w_id)
		conn.close()
	print(threading.current_thread().name, ": Stopped receiving updates")
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
