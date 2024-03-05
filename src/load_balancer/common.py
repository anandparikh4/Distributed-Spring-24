import sys

from aiodocker import Docker
from colorama import Fore, Style
from fifolock import FifoLock
from icecream import ic

from consts import *
from hash import ConsistentHashMap, requestHashList, serverHashList

# Lock to protect the replicas list
lock = FifoLock()

# Configure icecream output
ic.configureOutput(prefix='[LB] | ')

# Disable icecream debug messages if DEBUG is not set to true
if not DEBUG:
    ic.disable()


# List to store web server replica hostnames
replicas = ConsistentHashMap(
    request_hash=requestHashList[HASH_NUM],
    server_hash=serverHashList[HASH_NUM])


# Map to store heartbeat fail counts for each server replica.
heartbeat_fail_count: dict[str, int] = {}


# server ids
serv_ids: dict[str, int] = {
    "Server-0": 123456,
    "Server-1": 234567,
    "Server-2": 345678,
}  # Already 3 servers running


# Shard Name to ConsistentHashMap
# To be filled by the load balancer with use
shard_map: dict[str, list[str]] = {}

# Shard Name to FifoLock
# To be filled by the load balancer with use
shard_locks: dict[str, FifoLock] = {}
