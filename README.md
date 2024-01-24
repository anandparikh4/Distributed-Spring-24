# Distributed-Assignment-1

## Members
* Utsav Basu - 20CS30057
* Anamitra Mukhopadhyay - 20CS30064
* Ritwik Ranjan Mallik - 20CS10049
* Anand Manojkumar Parikh - 20CS10007

## Main Libraries Used
### Quart
There are two major specifications for interfacing web applications with web servers: WSGI (Web Server Gateway Interface) and ASGI (Asynchronous Server Gateway Interface). WSGI is a synchronous interface, meaning that it handles one request at a time per process or thread. On the other hand, ASGI supports handling multiple requests concurrently without blocking. Since the load balancer should be able to handle as many as 10,000 concurrent requests, a web application based on WSGI such as Flask is not suitable. Rather, Quart, a framework built on top of Flask supporting ASGI servers, is a better choice.

### asyncio
In Python, asyncio is a library to write concurrent code using async/await syntax. Many asynchronous Python frameworks that provide high-performance network and web servers, database connection libraries, distributed task queues, etc., are built on top of asyncio.

### aiohttp
The aiohttp library is used for building an asynchronous HTTP client/server for asyncio and Python. It supports both Server WebSockets and Client WebSockets.

### aiodocker
The aiodocker library is a simple Docker HTTP API wrapper written with asyncio and aiohttp. Rather than making system calls inside the load balancer to perform Docker-related tasks like adding or removing server containers, aiodocker exposes functions with loads of flexibilities to perform these tasks.

### fifolock
This library is used for controlling concurrent read/writes to two important data structures used in the application. One of these data structures is used for mapping requests to virtual servers by consistent hashing. The other maintains the count of failed heartbeats for the servers.

## Design Choices
Different design choices have been made throughout the application, keeping in mind efficiency, functionality, and understandability. Some of these are given below:

### Threading vs Asyncio
Both threading and asyncio libraries in Python support concurrent programming. However, the choice was made to use the asyncio library due to the following reasons:
1. Due to the Global Interpreter Lock, only one thread can execute Python code at once. This means that for most Python 3 implementations, the different threads do not actually execute at the same time; they merely appear to.
2. Threads are used to perform preemptive multitasking where the scheduler decides which threads to run and when. On the other hand, asyncio model allows cooperative multitasking where the user decides which tasks to run and when.

### Random Hostnames
When a GET request is made to the /add endpoint of the load balancer, the payload requires the number of servers `N` and a list of hostnames `replicas`. If `N` > `len(replicas)`, `N - len(replicas)` servers are to be added having random hostnames. A random hostname has the format `Server-XXX-YYY` where XXX is a three-digit random number and YYY is a three-digit timestamp based on the current time in milliseconds. The chances of two randomly generated hostnames having the same hostname, in practice, is quite low.

### Cooperative Multitasking
The application runs several tasks asynchronously. These are:
1. `spawn_container`: This task is used to spawn the server containers. The load balancer maintains a total of `N` servers at a time. If some server goes down, the load balancer detects and restarts it.
2. `stop_container`: This task is used to remove the spawned server containers. The load balancer sends a signal SIGINT to the running container to stop it. If the container does not stop within `STOP_TIMEOUT`, it sends a SIGKILL signal to remove the container altogether.
3. `get_heartbeat`: This task is used to periodically get the heartbeats of the server containers. Whenever a new server container is added, an entry is initialized as 0. This maintains the number of failed heartbeats for that container, initialized as 0. The server checks the heartbeats at an interval of `HEARTBEAT_INTERVAL` seconds. Upon not receiving the heartbeat for a server, the corresponding entry is increased. When this entry reaches `MAX_FAIL_COUNT`, the server is assumed to be down and is restarted. This task is run as a background task using Quart.
4. `handle_flatline`: This task is used to handle the flatline of the servers. When a server becomes unresponsive and is assumed to be down, this task restarts it.

Cooperation takes place whenever there are multiple tasks to perform simultaneously. For Docker-related tasks such as spawning and removing a container, the set of tasks are added to a pool and processed in batches of size `DOCKER_TASK_BATCH_SIZE`. This is done using a semaphore initialized to `DOCKER_TASK_SIZE`. For HTTP requests, similarly, the set of requests are processed in batches of size `REQUEST_BATCH_SIZE`. This is done using a semaphore initialized to `REQUEST_TASK_BATCH_SIZE`. Cooperation is ensured by calling `await asyncio.sleep(0)` inside a task in appropriate places, which gives other tasks a chance to run before it itself starts executing.
