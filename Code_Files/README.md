> **Execution**
>  Execute the files in the following order, on 5 separate terminals.
>  1. python3 master.py config.json <scheduling algorithm>                        # <scheduling algorithm> = RR or RANDOM or LL
      > Wait for the master to be initialized before procceeding.
>  2. python3 worker.py 4000 1
>  3. python3 worker.py 4001 2
>  4. python3 worker.py 4002 3
>  5. python3 requests.py <no_of_requests>
>  
>  ( 3, 4, 5 => python3 worker.py <port_number> <worker_id> )
  

> **File Manifest:**
> 1. config.json:
>   * Holds information on the configuration of workers -> worker_id, slots, port
> 2. requests.py:
>   * Generates job requests
> 3. master.py:
>   * Listens to Job Requests and Schedules tasks.
> 4. worker.py:
    * Executes tasks and notifies master on completion.
   
