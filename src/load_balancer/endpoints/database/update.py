from quart import Blueprint, jsonify, request

from utils import *

blueprint = Blueprint('update', __name__)


@blueprint.route('/update', methods=['PUT'])
async def update():
    """
    Update a particular data entry in the distributed database.

    If `stud_id` does not exist:
        Return an error message.

    `Request payload:`
        `stud_id: id of the student whose data is to be updated`
        `data: the new data of the student`
            `stud_id: student id`
            `stud_name: student name`
            `stud_marks: student marks`

    `Response payload:`
        `message: Data entry for stud_id: `stud_id` updated`
        `status: status of the request`

    `Error payload:`
        `message: error message`
        `status: status of the request`
    """

    await asyncio.sleep(0)

    async def update_put_wrapper(
        session: aiohttp.ClientSession,
        server_name: str,
        json_payload: dict
    ):

        # To allow other tasks to run
        await asyncio.sleep(0)

        async with session.put(f'http://{server_name}:5000/update',
                               json=json_payload) as response:
            await response.read()

        return response
    # END update_put_wrapper

    try:
        # Get the request payload
        payload = dict(await request.get_json())
        ic(payload)

        if payload is None:
            raise Exception('Payload is empty')

        # Get the required fields from the payload and check for errors
        stud_id = int(payload.get('stud_id', -1))

        if stud_id == -1:
            raise Exception('Payload does not contain `stud_id` field')

        data = dict(payload.get('data', {}))

        if len(data) == 0:
            raise Exception('Payload does not contain `data` field')

        if not all(k in data.keys()
                   for k in
                   ["stud_id", "stud_name", "stud_marks"]):
            raise Exception('Data entry is invalid')

        async with common.lock(Read):
            async with common.pool.acquire() as conn:
                async with conn.transaction(isolation='serializable'):
                    record = await conn.fetchrow(
                        '''--sql
                        SELECT
                            shard_id,
                            valid_at
                        FROM
                            ShardT
                        WHERE
                            (stud_id_low <= ($1::INTEGER)) AND
                            (($1::INTEGER) < stud_id_low + shard_size)
                        ''',
                        stud_id
                    )

                    if record is None:
                        raise Exception(f'stud_id {stud_id} does not exist')

                    shard_id: str = record["shard_id"]
                    shard_valid_at: int = record["valid_at"]

                    # TODO: Change to ConsistentHashMap
                    server_names = shard_map[shard_id].getServerList()
                    max_valid_at = shard_valid_at

                    async with shard_locks[shard_id](Write):
                        # Convert to aiohttp request
                        timeout = aiohttp.ClientTimeout(
                            connect=REQUEST_TIMEOUT)
                        async with aiohttp.ClientSession(timeout=timeout) as session:
                            tasks = [asyncio.create_task(
                                update_put_wrapper(
                                    session,
                                    server_name,
                                    json_payload={
                                        "shard": shard_id,
                                        "stud_id": stud_id,
                                        "data": data,
                                        "valid_at": shard_valid_at
                                    }
                                )
                            ) for server_name in server_names]

                            serv_response = await asyncio.gather(*tasks, return_exceptions=True)
                            serv_response = [None if isinstance(r, BaseException)
                                             else r for r in serv_response]
                        # END async with aiohttp.ClientSession

                        max_valid_at = shard_valid_at
                        # If all replicas are not updated, then return an error
                        for r in serv_response:
                            if r is None or r.status != 200:
                                raise Exception('Failed to update data entry')

                            resp = dict(await r.json())
                            cur_valid_at = int(resp["valid_at"])
                            max_valid_at = max(max_valid_at, cur_valid_at)
                        # END for r in serv_response

                        await conn.execute(
                            '''--sql
                            UPDATE
                                ShardT
                            SET
                                valid_at = ($1::INTEGER)
                            WHERE
                                shard_id = ($2::TEXT)
                            ''',
                            max_valid_at,
                            shard_id,
                        )
                    # END async with shard_locks[shard_id](Write)
                # END async with conn.transaction()
            # END async with common.pool.acquire() as conn
        # END async with common.lock(Read)

        # Return the response payload
        return jsonify(ic({
            'message': f"Data entry for stud_id: {stud_id} updated",
            'status': 'success'
        })), 200

    except Exception as e:

        return jsonify(ic(err_payload(e))), 400
    # END try-except
