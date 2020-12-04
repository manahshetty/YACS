# YACS - Yet Another Centralized Scheduler

This project deals with a simulation of a centralized scheduling framework, and also performs analysis on the performance overhead and number of tasks processed by each worker, for all three algorithms. 

The framework uses three different scheduling algorithms - Random Scheduling, Round Robin scheduling and Least Loaded scheduling, in order to determine what task to send to which worker node (here, 3 workers have been used along with one master). 

The master receives a job request, consisting of several tasks. Each of these tasks is assigned to a worker based on the scheduling algorithm. Once a task is completed, the worker sends the update to the master, which then removes the task from the scheduling pool.

### Requirements

- pandas
- numpy
- matplotlib
- seaborn

### Run

Execute the following simultaneously on five different terminals 

```python3 master.py config.json <algo>```

```python3 worker.py 4000 1```
```python3 worker.py 4001 2 ```
```python3 worker.py 4002 3```

```python3 requests.py <number of requests>```



### Authors
- Adithi Satish
- Manah Shetty
- Saurav Nayak
- Shreya Shukla
