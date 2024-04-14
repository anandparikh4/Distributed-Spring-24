from quart import Blueprint, jsonify, request
import common
from common import *
import aiohttp
from .rules import bookkeeping

blueprint = Blueprint('delete', __name__)

async def del_put_wrapper(
    session: aiohttp.ClientSession,
    server_name: str,
    json_payload: dict
):
    # To allow other tasks to run
    await asyncio.sleep(0)

    async with session.delete(f'http://{server_name}:5000/del',
                                json=json_payload) as response:
        await response.read()

    return response

@blueprint.route('/del', methods=['DELETE'])
async def delete():
    """
        Delete data entries from the database

        Request payload:
            "shard"             : <shard_id>
            "term"              : <term>
            "stud_id"           : <stud_id>
            "is_primary"        : true/false (optional)
            "secondary_servers" : ["server1", ...]

        Response payload:
            "message"   : Data entry with stud_id:<stud_id> removed
            "status"    : "success"
    """

    try:
        # Get the request payload
        payload: dict = await request.get_json()
        ic(payload)

        shard_id = str(payload.get('shard', ""))
        term = int(payload.get('term', -1))
        is_primary = str(payload.get('is_primary', 'false')).lower() == 'true'
        secondary_servers = list(payload.get('secondary_servers', []))
        stud_id = int(payload.get('stud_id', ""))

        # perform bookkeeping
        await bookkeeping(shard_id, term, "d")

        # insert log into LogT and update TermT
        async with common.pool.acquire() as conn:
            async with conn.transaction():
                # add unexecuted term to TermT
                await conn.execute('''--sql
                    UPDATE TermT
                    SET last_idx = $2 , executed = FALSE
                    WHERE shard_id = $1
                ''',shard_id,term)

                # add latest log to LogT
                await conn.execute('''--sql
                    INSERT INTO LogT (log_idx , shard_id , operation , stud_id , content)
                    VALUES ($1::INTEGER,
                            $2::TEXT,
                            $3::TEXT,
                            $4::INTEGER,
                            NULL);
                    ''',term,shard_id,"d",stud_id)
                
                if is_primary:
                    timeout = aiohttp.ClientTimeout(connect=REQUEST_TIMEOUT)
                    async with aiohttp.ClientSession(timeout=timeout) as session:
                        tasks = [asyncio.create_task(
                                del_put_wrapper(
                                    session=session,
                                    server_name=server_name,
                                    json_payload={
                                        "shard"     : shard_id,
                                        "term"      : term,
                                        "stud_id"   : stud_id,
                                        "is_primary": False
                                    }
                                )
                            ) for server_name in secondary_servers]

                        serv_response = await asyncio.gather(*tasks, return_exceptions=True)
                        serv_response = [None if isinstance(r, BaseException)
                                              else r for r in serv_response]

                        # If not all replicas are updated, then return an error
                        for r in serv_response:
                            if r is None or r.status != 200:
                                raise Exception(
                                    'Failed to write all data entries')

        response_payload = {
            "message": f'Data entry with Stud_id:{stud_id} removed',
            "status": "success"
        }
        return jsonify(ic(response_payload)), 200

    except Exception as e:
        print(f'{Fore.RED}ERROR | '
              f'Error in data_write: {e}'
              f'{Style.RESET_ALL}',
              file=sys.stderr)
        return jsonify(ic(err_payload(e))), 400
