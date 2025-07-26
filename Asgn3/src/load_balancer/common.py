from __future__ import annotations

import asyncio
import random
import sys
from typing import Any, Dict, List, Optional, Set, Tuple

import aiohttp
import asyncpg
from colorama import Fore, Style
from icecream import ic

from consts import *


# Postgres connection pool
pool: asyncpg.Pool[asyncpg.Record]


# Configure icecream output
ic.configureOutput(prefix='[LB] | ')


# Disable icecream debug messages if DEBUG is not set to true
if not DEBUG:
    ic.disable()


# seed random number generator in DEBUG mode
if DEBUG:
    random.seed(RANDOM_SEED)
