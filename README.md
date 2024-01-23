# Distributed-Assignment-1

## Members
* Utsav Basu - 20CS30057
* Anamitra Mukhopadhyay - 20CS30064
* Ritwik Ranjan Mallik - 20CS10049
* Anand Manojkumar Parikh - 20CS10007

## Main Libraries Used
### Quart
There are two major specifications for interfacing web applications with web servers: WSGI (Web Server Gateway Interface) and ASGI (Asynchronous Server Gateway Interface). WSGI is a synchronous interface, meaning that it handles one request at a time per process or thread. On the other hand, ASGI supports handling multiple requests concurrently without blocking. Since the load balancer should be able to handle as many as 10,000 concurrent requests, an web application based on WSGI such as Flask is not suitable. Rather, Quart, a framework built on top of Flask supporting ASGI servers is a better choice.

### asyncio
In python, asyncio is a library to write concurrent code using async/await syntax. Many asynchronous python frameworks that provide high-performance network and web-servers, database connection libraries, distributed task queues etc. are built on top of asyncio.

### aiohttp
The aiohttp library is used for bulilding asynchronous HTTP client/server for asyncio and Python. It supports both Server WebSockets and Client WebSockets. 

### aiodocker
The aiodocker library is a simple docker HTTP API wrapper written with asyncio and aiohttp. Rather than making system calls inside the load balancer to perform docker related tasking like adding or removing server containers, aiodocker exposes functions with loads of flexibilities to perform these tasks.

### fifolock
This library is used for controlling concurrent read/writes to two important data structures used in the application. One of these data structures is used for mapping requests to virtual servers by consistent hasing. The other maintains the count of failed heartbeats for the servers.  

## Design Choices
Different design choices have been made throughout the application, keeping in mind efficiency, functionality and understandability. Some of these are given below:

### Threading vs Asyncio
Both threading and asyncio libraries in Python support concurrent programming. However, choice was made to use the asyncio library due to the following reasons:
1. Due to the Global Interpreter Lock, only one thread can execute Python code at once. This means that for most Python 3 implementations, the different threads do not actually execute at the same time, they merely appear to.
2. Threads are used to perform preemptive multi-tasking where the scheduler decides which threads to run and when. On the other hand, asyncio model allows cooperative multi-tasking where the user decides which tasks to run and when.

### Random Hostnames
When a GET request is made to the /add endpoint of the load balancer, the payload requires the number of servers `N` and list of hostnames `replicas`. If `N` > `len(replicas)`, `N - len(replicas)` servers are to be added having random hostnames. A random hostname has the format `Server-XXX-YYY` where XXX is a three-digit random number and YYY is a three-digit timestamp based on the current time in milliseconds. The chances of two randomly generated hostnames having the same hostname, in practice, is quite low.
