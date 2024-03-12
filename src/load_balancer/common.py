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
serv_ids = {
    "Server-0": 123456,
    "Server-1": 234567,
    "Server-2": 345678,
}  # Already 3 servers running


# Shard Name to ConsistentHashMap
shard_map: dict[str, list[str]] = {"sh1":[]} # TODO: change list[str] to ConsistentHashMap

# Shard Name to Shard Lock
shard_locks: dict[str, FifoLock] = {}
