import aiohttp
from quart import Blueprint, jsonify, request

import common
from common import *

from .rules import bookkeeping

blueprint = Blueprint('update', __name__)


async def update_put_wrapper(
    session: aiohttp.ClientSession,
    server_name: str,
    json_payload: dict
):

    # To allow other tasks to run
    await asyncio.sleep(0)

    async with session.post(f'http://{server_name}:5000/update',
                            json=json_payload) as response:
        await response.read()

    return response


@blueprint.route('/update', methods=['POST'])
async def data_write():
    """
        Update data entries in the database

        Request payload:
            "shard"             : <shard_id>
            "term"              : <term>
            "stud_id"           : <stud_id>
            "data"              : {"stud_id": <stud_id>, "stud_name": <stud_name>, "stud_marks": <stud_marks>}
            "is_primary"        : true/false (optional)
            "secondary_servers" : ["server1", ...]

        Response payload:
            "message": Data entry for stud_id:<stud_id> updated
            "status": "success"
    """

    try:
        payload: dict = await request.get_json()
        ic(payload)

        # decode payload
        shard_id = str(payload.get('shard', ""))
        term = int(payload.get('term', -1))
        data = dict(payload.get('data', {}))
        is_primary = str(payload.get('is_primary', 'false')).lower() == 'true'
        secondary_servers = list(payload.get('secondary_servers', []))
        stud_id = int(payload.get('stud_id', -1))

        content = {
            str(stud_id): [str(data["stud_name"]), int(data["stud_marks"])]}

        # perform bookkeeping
        await bookkeeping(shard_id, term, "u")

        # insert log into LogT and update TermT
        async with common.pool.acquire() as conn:
            async with conn.transaction():

                check = conn.transaction()
                await check.start()
                try:
                    await conn.execute('''--sql
                            UPDATE StudT
                            SET stud_name = $3::TEXT , stud_marks = $4::INTEGER
                            WHERE shard_id = $1::TEXT
                            AND stud_id = $2::INTEGER
                        ''',
                        shard_id, stud_id,
                        str(content[str(stud_id)][0]),
                        int(content[str(stud_id)][1]))
                finally:
                    await check.rollback()

                # add unexecuted term to TermT
                await conn.execute('''--sql
                    UPDATE TermT
                    SET last_idx = $2 , executed = FALSE
                    WHERE shard_id = $1
                ''', shard_id, term)

                # add latest log to LogT
                await conn.execute('''--sql
                    INSERT INTO LogT (log_idx , shard_id , operation , stud_id , content)
                    VALUES ($1::INTEGER,
                            $2::TEXT,
                            $3::TEXT,
                            $4::INTEGER,
                            $5::JSON);
                    ''', term, shard_id, "u", stud_id, json.dumps(content))

                if is_primary:
                    timeout = aiohttp.ClientTimeout(connect=REQUEST_TIMEOUT)
                    async with aiohttp.ClientSession(timeout=timeout) as session:
                        tasks = [asyncio.create_task(
                            update_put_wrapper(
                                session=session,
                                server_name=server_name,
                                json_payload={
                                    "shard": shard_id,
                                    "term": term,
                                    "stud_id": stud_id,
                                    "data": data,
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

        # Send the response
        response_payload = {
            "message": f"Data entry for stud_id:{data['stud_id']} updated",
            "status": "success"
        }
        return jsonify(ic(response_payload)), 200

    except Exception as e:
        return jsonify(ic(err_payload(e))), 400
