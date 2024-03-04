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


# server unique id generator (TODO: change this asap)
serv_id = 3  # already have 3 servers running
