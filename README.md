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
