from quart import Blueprint, jsonify, request

from utils import *

blueprint = Blueprint('delete', __name__)


@blueprint.route('/del', methods=['DELETE'])
async def delete():
    """
    Delete a particular data entry in the distributed database.

    If `stud_id` does not exist:
        Return an error message.

    `Request payload:`
        `stud_id: id of the student whose data is to be deleted`

    `Response payload:`
        `message: Data entry with stud_id: `stud_id` removed for all replicas`
        `status: status of the request`

    `Error payload:`
        `message: error message`
        `status: status of the request`
    """

    await asyncio.sleep(0)

    async def del_put_wrapper(
        session: aiohttp.ClientSession,
        server_name: str,
        json_payload: Dict
    ):
        # To allow other tasks to run
        await asyncio.sleep(0)

        async with session.delete(f'http://{server_name}:5000/del',
                                  json=json_payload) as response:
            await response.read()

        return response
    # END del_put_wrapper

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

        # Get the shard name containing the entry

        async with lock(Read):
            async with pool.acquire() as conn:
                async with conn.transaction():
                    record = await conn.fetchrow(
                        '''--sql
                        SELECT
                            shard_id,
                            valid_at
                        FROM
                            ShardT
                        WHERE
                            (stud_id_low <= ($1::INTEGER)) AND
                            (($1::INTEGER) <= stud_id_low + shard_size)
                        ''',
                        stud_id)

                    if record is None:
                        raise Exception(f'stud_id {stud_id} does not exist')

                    shard_id: str = record["shard_id"]
                    shard_valid_at: int = record["valid_at"]

                    # TODO: Change to ConsistentHashMap
                    server_names = shard_map[shard_id]

                    async with shard_locks[shard_id](Write):
                        # Convert to aiohttp request
                        timeout = aiohttp.ClientTimeout(
                            connect=REQUEST_TIMEOUT)
                        async with aiohttp.ClientSession(timeout=timeout) as session:
                            tasks = [asyncio.create_task(del_put_wrapper(
                                session,
                                server_name,
                                json_payload={
                                    "shard": shard_id,
                                    "stud_id": stud_id,
                                    "valid_at": shard_valid_at
                                }
                            )) for server_name in server_names]
                            serv_response = await asyncio.gather(*tasks, return_exceptions=True)
                            serv_response = serv_response[0] if not isinstance(
                                serv_response[0], BaseException) else None
                        # END async with aiohttp.ClientSession

                        if serv_response is None:
                            raise Exception('Server did not respond')

                        serv_response = dict(await serv_response.json())
                        cur_valid_at = int(serv_response["valid_at"])

                        max_valid_at = max(shard_valid_at, cur_valid_at)

                        await conn.execute(
                            '''--sql
                            UPDATE
                                ShardT
                            SET
                                valid_at = ($1::INTEGER)
                            WHERE
                                shard_id = ($2::INTEGER)
                            ''',
                            max_valid_at,
                            shard_id,
                        )
                    # END async with shard_locks[shard_id](Write)
                # END async with conn.transaction()
            # END async with pool.acquire() as conn
        # END async with lock(Read)

        # Return the response payload
        return jsonify(ic({
            'message': f"Data entry with stud_id: {stud_id} removed from all replicas",
            'status': 'success'
        })), 200

    except Exception as e:
        if DEBUG:
            print(f'{Fore.RED}ERROR | '
                  f'{e}'
                  f'{Style.RESET_ALL}',
                  file=sys.stderr)

        return jsonify(ic(err_payload(e))), 400
    # END try-except
