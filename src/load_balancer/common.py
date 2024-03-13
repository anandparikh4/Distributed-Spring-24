import asyncio
import random
import sys
import time


import aiohttp
import asyncpg
from aiodocker import Docker
from colorama import Fore, Style
from fifolock import FifoLock
from icecream import ic
from typing import List, Dict, Any, Tuple, Set

from consts import *
from hash import ConsistentHashMap, requestHashList, serverHashList

# Lock to protect the replicas list
lock = FifoLock()

# Postgres connection pool
pool: asyncpg.Pool[asyncpg.Record]

# Configure icecream output
ic.configureOutput(prefix='[LB] | ')

# Disable icecream debug messages if DEBUG is not set to true
if not DEBUG:
    ic.disable()


# List to store web server replica hostnames
replicas = ConsistentHashMap()


# Map to store heartbeat fail counts for each server replica.
heartbeat_fail_count: Dict[str, int] = {}


# server ids
serv_ids: Dict[str, int] = {}


# Shard Name to ConsistentHashMap
# To be filled by the load balancer with use
# TODO: change to Dict[str, ConsistentHashMap]
shard_map: Dict[str, ConsistentHashMap] = {}


# Shard Name to FifoLock
# To be filled by the load balancer with use
shard_locks: Dict[str, FifoLock] = {}
