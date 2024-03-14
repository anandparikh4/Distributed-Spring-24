from __future__ import annotations

import asyncio
import sys

import asyncpg
from colorama import Fore, Style
from icecream import ic

from consts import *

ic.configureOutput(prefix=f'[{HOSTNAME}: {SERVER_ID}] | ')

# Disable icecream debug messages if DEBUG is not set to true
if not DEBUG:
    ic.disable()


# Postgres connection pool
pool: asyncpg.Pool[asyncpg.Record]


def err_payload(err: Exception):
    """
    Generate an error payload.
    """
    
    if DEBUG:
        print(f'{Fore.RED}ERROR | '
              f'{err.__class__.__name__}: {err}'
              f'{Style.RESET_ALL}',
              file=sys.stderr)

    return {
        'message': f'<Error> {err.__class__.__name__}: {err}',
        'status': 'failure'
    }
