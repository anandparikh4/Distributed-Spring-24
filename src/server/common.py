from icecream import ic
import asyncpg
import asyncio

from consts import *

ic.configureOutput(prefix=f'[{HOSTNAME}: {SERVER_ID}] | ')

# Disable icecream debug messages if DEBUG is not set to true
if not DEBUG:
    ic.disable()

pool: asyncpg.Pool[asyncpg.Record]

def err_payload(err: Exception):
    """
    Generate an error payload.
    """

    return {
        'message': f'<Error> {err}',
        'status': 'failure'
    }