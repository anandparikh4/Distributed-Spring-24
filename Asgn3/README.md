# Distributed-Assignment-3

## Members
* Utsav Basu - 20CS30057
* Anamitra Mukhopadhyay - 20CS30064
* Ritwik Ranjan Mallik - 20CS10049
* Anand Manojkumar Parikh - 20CS10007

## How to run
Before starting, permissions for `docker.sock` have to be updated.
```shell
sudo chmod 777 <location of docker.sock> # usually the location is /var/run/docker.sock
```

There is a [Makefile](./src/Makefile) in the `./src` directory.
To start everything:
```shell
make
```
To stop everything:
```shell
make stop
```

## Analysis

For A1-A3, the read times and write times are checked on two different machines. The payloads can be found inside [read_write_testing](./src/client/read_write_testing).

### A1
|           | Read Time (in s) | Write Time (in s) |
|-----------|------------------|-------------------|
| Machine 1 | 433.89           | 1577.40            |
| Machine 2 | 309.94           | 2234.44            |

### A2
|           | Read Time (in s) | Write Time (in s) |
|-----------|------------------|-------------------|
| Machine 1 | 437.80           | 4020.94            |
| Machine 2 | 274.79           | 2838.72            |

### A3
|           | Read Time (in s) | Write Time (in s) |
|-----------|------------------|-------------------|
| Machine 1 | 425.43           | 3251.08            |
| Machine 2 | 287.27           | 3247.10            |

### A4
The prerequisites and sample request-reponse pairs for the endpoints can be found inside [endpoint_testing](./src/client/endpoint_testing).

## Data Generation
In the `./src/client/data` directory, there are two files [fnames.txt](./src/client/data/fnames.txt) and [lnames.txt](./src/client/data/lnames.txt). `fnames.txt` contains `942` unique first names and `lnames.txt` contains `995` unique last names. Using these names, [generate.py](./src/client/generate.py) creates `942 * 995 = 937290` data entries with marks generated unformly at random in the range from `1` to `100`. 

## Main Libraries Used
### [Quart](https://pgjones.gitlab.io/quart/)
There are two major specifications for interfacing web applications with web servers: [WSGI (Web Server Gateway Interface)](https://wsgi.readthedocs.io/en/latest/what.html) and [ASGI (Asynchronous Server Gateway Interface)](https://asgi.readthedocs.io/en/latest/). WSGI is a synchronous interface, meaning that it handles one request at a time per process or thread. On the other hand, ASGI supports handling multiple requests concurrently without blocking. Since the load balancer should be able to handle as many as 10,000 concurrent requests, an web application based on WSGI such as [Flask](https://flask.palletsprojects.com/en/3.0.x/) is not suitable. Rather, Quart, a framework built on top of Flask supporting ASGI servers is a better choice.

### [asyncio](https://docs.python.org/3/library/asyncio.html)
In python, asyncio is a library to write concurrent code using async/await syntax. Many asynchronous python frameworks that provide high-performance network and web-servers, database connection libraries, distributed task queues etc. are built on top of asyncio.

### [aiohttp](https://docs.aiohttp.org/en/stable/)
The aiohttp library is used for bulilding asynchronous HTTP client/server for asyncio and Python. It supports both Server WebSockets and Client WebSockets. 

### [aiodocker](https://aiodocker.readthedocs.io/en/latest/)
The aiodocker library is a simple docker HTTP API wrapper written with asyncio and aiohttp. Rather than making system calls inside the load balancer to perform docker related tasking like adding or removing server containers, aiodocker exposes functions with loads of flexibilities to perform these tasks.

### [fifolock](https://github.com/michalc/fifolock)
This library is used for controlling concurrent read/writes to two important data structures used in the application. One of these data structures is used for mapping requests to virtual servers by consistent hasing. The other maintains the count of failed heartbeats for the servers.

### [asyncpg](https://magicstack.github.io/asyncpg/current/)
Asyncpg is a high-performance, asynchronous PostgreSQL database client library for Python. It is designed to provide efficient and low-latency communication with a PostgreSQL database by leveraging the power of Python's asyncio framework.

## Design Choices
Different design choices have been made throughout the application, keeping in mind efficiency, functionality and understandability. Some of these are given below:

### Threading vs Asyncio
Both threading and asyncio libraries in Python support concurrent programming. However, choice was made to use the asyncio library due to the following reasons:
1. Due to the Global Interpreter Lock, only one thread can execute Python code at once. This means that for most Python 3 implementations, the different threads do not actually execute at the same time, they merely appear to.
1. Threads are used to perform preemptive multi-tasking where the scheduler decides which threads to run and when. On the other hand, asyncio model allows cooperative multi-tasking where the user decides which tasks to run and when.

### Cooperative Multitasking
The application runs several taks asynchronously. These are:
1. `spawn_container`: This task is used to spawn the server containers. The load balancer maintains a total of `N` servers at a time. If some server goes down, the load balancer detects it and spawns a new server.
1. `remove_container`: This task is used to remove the spawned server containers. The load balancer sends a signal SIGINT to the running container to stop it. If the container does not stop within `STOP_TIMEOUT`, it sends a SIGKILL signal to remove the container altogether. 
1. `get_heartbeat`: This task is used to periodically get the heartbeats of the server containers. Whenever a new server container is added, an entry is initialized as 0. This maintains the number of failed heartbeats for that container. The load balancer checks the heartbeats at an interval of `HEARTBEAT_INTERVAL` seconds. Upon not receiving the heartbeat for a server, the corresponding entry is increased. When this entry reaches `MAX_FAIL_COUNT`, the server is assumed to be down and is restarted. This task is run as background task using Quart.
1. `handle_flatline`: This task is used to handle the flatline of the servers. When a server becomes unresponsive and is assumed to be down, this task respawns a new server. This new server replaces the failed server. 

A cooperation takes place whenever there are multiple tasks to perform simultaneously. For docker related tasks such as spawning and removing a container, the set of tasks are added to to a pool and processed in batches of size `DOCKER_TASK_BATCH_SIZE`. This is done using a semaphore initialized to `DOCKER_TASK_BATCH_SIZE`. For http requests, similarly, the set of requests are processed in batches of size `REQUEST_BATCH_SIZE`. This is done using a semaphore initialized to `REQUEST_BATCH_SIZE`. Cooperation is ensured by calling `await asyncio.sleep(0)` inside a task in appropiate places, which gives other tasks a chance to run before it itself starts executing.

### Dockerfile
Some design choices have been made during dockerizing the application. These are as follows:
1. The default python image is based on the Ubuntu image which installs some unnecessary packages not needed for our application. Instead, the python image based on the Alpine image is used. This image is much lighter than the earlier one.
1. Multistage build have been used during dockerization. This is done to reduce the image sizes for the load balancer and the server as much as possible.
1. Both the load balancer and server containers are deployed via a shell script `deploy.sh`. `postgres` is first run as a background task and the main process waits until it starts, using a variable `pg_isready`. When `postgres` is up and running, the main process runs the actual python file as a background task - `load_balancer.py` for load balancer and `server.py` for servers. Signal handlers are modified such that any `SIGTERM` or `SIGINT` signal to the main process is sent to the two child processes as well. The main process then waits for the two child processes.  

### The Protocol

#### Write-Ahead Logging
For ensuring distributed database consistency, a Write-Ahead Logging (WAL) mechanism is used. WAL works as follows:
1. **Logging Changes Before Writing to Disk**: With WAL, any changes to the database are first recorded in a log file before they are applied to the database files on disk. This log file is stored in a durable storage medium (like SSDs, RAID-configured HDDs or SSDs, and cloud storage).
2. **Sequential Writes**: The changes are written sequentially to the log, which is more efficient than random disk access, especially for large transactions or when multiple transactions are occurring simultaneously.
3. **Recovery Process**: In the event of a crash or restart, the system reads the WAL file(s) to redo operations that were not fully committed to the database files, ensuring that no committed transactions are lost. It can also undo any changes from transactions that were not completed, maintaining data integrity.
4. **Replication Log Shipping**: A primary database server is chosen for each shard when writing to that shard is going to happen. The primary database server writes changes to its WAL as part of normal operations. These WAL records can then be shipped to replica servers, where they are replayed to apply the same changes. This ensures that all replicas process the same set of changes in the same order, maintaining consistency across the system.
5. **Synchronous Replication**: The primary waits for the majority replica to commit the data before committing its copy of data and acknowledging transactions, which ensures strong consistency. If the primary shard fails, then a new primary is chosen from the replicated shards having the most updated log entries. As soon as the downed database server is up, it copies all the shards from the primary shards. It ensures the system can be recovered from crash failure
with consistent data.

#### Protocol
A server maintains three tables:
1. `StudT(stud_id: INTEGER, stud_name: TEXT, stud_marks: INTEGER, shard_id: TEXT)`
1. `TermT(shard_id: TEXT, last_idx: INTEGER, executed: BOOLEAN)`
2. `logT(log_idx: INTEGER, shard_id: TEXT, operation: TEXT, stud_id: INTEGER, content: JSON)`

The load balancer maintains a single table:
1. `ShardT(stud_id_low: INTEGER, shard_id: TEXT, shard_size: INTEGER, valid_at: INTEGER)`

A particular shard is stored in a number of servers. In this group, one server acts as the primary server and rest are secondary servers for that particular shard. Upon receiving a request the load balancer first queries the shard manager to find out primaries for the relevant shards. The load balancer sends the request to the primary server which forwards it to the secondary servers.

Bookkeeping operations are performed by the servers upon receiving a request for a particular shard. The requests can be assumed to have a general form `(shard_id, term, op)`. Bookkeeping happens in the following fashion:
1. The server consults its `logT` table to find all entries corresponding to `shard_id`.
2. For entries where (`term` > `last_idx`) or (`term` == `last_idx` and `op` == 'R'), if the log entry is not executed, it is executed.
3. For entries where (`term` < `last_idx`) or (`term` == `last_idx` and `op` != 'R'), the entry is removed from the log.