from quart import current_app
from colorama import Fore, Style
import sys

from consts import *
from common import *

# All rules for enforcing synchronization between shard replicas across servers
# Simply do 'await rules(payload)' in each operation. No return value

async def rules(payload: dict = {}):
    """
        Rule 1 : Erase all entries where created_at > valid_idx or deleted_at <= valid_idx
        Rule 2 : Update deleted_at = null all entries where deleted_at > valid_idx
    """

    try:
        # Get shard id and valid_idx from payload
        shard_id = payload.get("shard_id", -1)
        valid_idx = payload.get("valid_idx", -1)

        # Enforce rules by executing database operations
        async with current_app.pool.acquire() as connection:
            async with connection.transaction():

                rule1 = connection.prepare('''
                    DELETE FROM StudT
                    WHERE (shard_id = $1)
                    AND (created_at > $2 OR deleted_at <= $2);
                ''')
                await connection.execute(rule1 , shard_id , valid_idx)

                rule2 = connection.prepare('''
                    UPDATE StudT
                    SET deleted_at = NULL
                    WHERE shard_id = $1
                    AND deleted_at > $2
                ''')
                await connection.execute(rule2 , shard_id , valid_idx)

    except Exception as e:
        if DEBUG:
            print(f'{Fore.RED}ERROR | '
                  f'{e}'
                  f'{Style.RESET_ALL}',
                  file=sys.stderr)
