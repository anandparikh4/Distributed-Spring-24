from quart import current_app
from colorama import Fore, Style
import sys

from consts import *
from common import *

# All rules for enforcing synchronization between shard replicas across servers
# Simply do 'await rules(payload)' in each operation. No return value

async def rules(payload: dict = {}):
    """
        Rule 1 : Erase all entries where cat > vat or dat <= vat
        Rule 2 : Update dat = null all entries where dat > vat
    """

    try:
        # Get shard id and vat from payload
        shard_id = payload.get("shard_id", -1)
        vat = payload.get("vat", -1)

        # Enforce rules by executing database operations
        async with current_app.pool.acquire() as connection:
            async with connection.transaction():

                rule1 = connection.prepare('''
                    DELETE FROM StudT
                    WHERE (shard_id = $1)
                    AND (cat > $2 OR dat <= $2);
                ''')
                await connection.execute(rule1 , shard_id , vat)

                rule2 = connection.prepare('''
                    UPDATE StudT
                    SET dat = NULL
                    WHERE shard_id = $1
                    AND dat > $2
                ''')
                await connection.execute(rule2 , shard_id , vat)

    except Exception as e:
        if DEBUG:
            print(f'{Fore.RED}ERROR | '
                  f'{e}'
                  f'{Style.RESET_ALL}',
                  file=sys.stderr)
