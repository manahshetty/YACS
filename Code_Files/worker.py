import socket
import sys
import json
import threading
import time

# Connect all sockets
# 5001 - Send updates
# 4000 - Receive tasks


def sendUpdate(w_id):
	w_id = str(w_id)
	jUSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	jUSocket.connect(("localhost", 5001))
	jUSocket.send(w_id.encode())
	print("Sent update\n")
	jUSocket.close()
def worker1(port, w_id):
	while(1):
		taskLaunchSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		print("Trying to connect to 4000")
		taskLaunchSocket.connect(("localhost", port))
		print("Connected to 4000")
		r = taskLaunchSocket.recv(1024)
		req = ""
		while r:
			req += r.decode()
			r = taskLaunchSocket.recv(1024)
		print(threading.current_thread().name, ": Request = ", req, "\n\n")
		if(req):
			request = json.loads(req)
			print("\t\tWorker ", w_id, ": ", request['task_id'], "\n")
			print("Calling sendUpdate.")
			start_time = time.time()
			end_time = start_time + request["duration"]

			while(end_time > time.time()):
				continue
			
			sendUpdate(w_id)
			print("Returned from Send update")
		else:
			break
		taskLaunchSocket.close()
	print("Stop Launching tasks")


with open(sys.argv[1]) as f:
	config = json.load(f)
	
t1 = threading.Thread(target = worker1, args = (config['workers'][0]['port'],config['workers'][0]['worker_id']))

t2 = threading.Thread(target = worker1, args = (config['workers'][1]['port'],config['workers'][1]['worker_id']))

t3 = threading.Thread(target = worker1, args = (config['workers'][2]['port'],config['workers'][0]['worker_id']))

t1.start()

t2.start()
t3.start()

t1.join()

t2.join()
t3.join()

#jUSocket.close()
#taskLaunchSocket.close()
