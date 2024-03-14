from quart import Blueprint, jsonify, request

from utils import *

blueprint = Blueprint('write', __name__)


@blueprint.route('/write', methods=['POST'])
async def write():
    """
    Write data entries in the distributed database.

    `Request payload:`
        `data: list of entries to be written to the distributed database`
            `stud_id: student id`
            `stud_name: student name`
            `stud_marks: student marks`

    `Response payload:`
        `message: `len(data)` data entries added`
        `status: status of the request`

    `Error payload:`
        `message: error message`
        `status: status of the request`
    """

    await asyncio.sleep(0)

    async def write_post_wrapper(
        session: aiohttp.ClientSession,
        server_name: str,
        json_payload: Dict
    ):
        # To allow other tasks to run
        await asyncio.sleep(0)

        async with session.post(f'http://{server_name}:5000/write',
                                json=json_payload) as response:
            await response.read()

        return response
    # END write_post_wrapper

    try:
        # Get the request payload
        payload = dict(await request.get_json())
        ic(payload)

        if payload is None:
            raise Exception('Payload is empty')

        # Get the required fields from the payload
        data: List[Dict] = list(payload.get('data', []))

        if len(data) == 0:
            raise Exception('Payload does not contain `data` field')

        # Check if the data entries are valid and compute the low and high ids
        for entry in data:
            if not all(k in entry.keys()
                       for k in
                       ["stud_id", "stud_name", "stud_marks"]):
                raise Exception('Data entry is invalid')
        # END for entry in data

        # Get the shard names and valid ats and the corresponding entries to be added
        # [shard_at] -> (list of entries, valid_at)
        shard_data: Dict[str, Tuple[List[Dict[str, Any]], int]] = {}

        async with common.lock(Read):
            async with common.pool.acquire() as conn:
                async with conn.transaction(isolation='serializable'):
                    get_shard_info_stmt = await conn.prepare(
                        '''--sql
                        SELECT
                            shard_id,
                            valid_at
                        FROM
                            ShardT
                        WHERE
                            (stud_id_low <= ($1::INTEGER)) AND
                            (($1::INTEGER) < stud_id_low + shard_size)
                        ''')

                    update_shard_info_stmt = await conn.prepare(
                        '''--sql
                        UPDATE
                            ShardT
                        SET
                            valid_at = ($1::INTEGER)
                        WHERE
                            shard_id = ($2::TEXT)
                        ''')

                    for entry in data:
                        stud_id = int(entry["stud_id"])
                        record = await get_shard_info_stmt.fetchrow(stud_id)

                        if record is None:
                            raise Exception(
                                f'stud_id {stud_id} does not exist')

                        shard_id: str = record["shard_id"]
                        shard_valid_at: int = record["valid_at"]

                        if shard_id not in shard_data:
                            shard_data[shard_id] = ([], shard_valid_at)

                        shard_data[shard_id][0].append(entry)

                    for shard_id in shard_data:
                        # TODO: Chage to ConsistentHashMap
                        server_names = shard_map[shard_id].getServerList()

                        async with shard_locks[shard_id](Write):
                            # Convert to aiohttp request
                            timeout = aiohttp.ClientTimeout(
                                connect=REQUEST_TIMEOUT)
                            async with aiohttp.ClientSession(timeout=timeout) as session:
                                tasks = [asyncio.create_task(
                                    write_post_wrapper(
                                        session,
                                        server_name,
                                        json_payload={
                                            "shard": shard_id,
                                            "data": shard_data[shard_id][0],
                                            "valid_at": shard_data[shard_id][1]
                                        }
                                    )
                                ) for server_name in server_names]

                                serv_response = await asyncio.gather(*tasks, return_exceptions=True)
                                serv_response = [None if isinstance(r, BaseException)
                                                 else r for r in serv_response]
                            # END async with aiohttp.ClientSession

                            max_valid_at = shard_data[shard_id][1]
                            # If all replicas are not updated, then return an error
                            for r in serv_response:
                                if r is None or r.status != 200:
                                    raise Exception(
                                        'Failed to write all data entries')

                                resp = dict(await r.json())
                                cur_valid_at = int(resp["valid_at"])
                                max_valid_at = max(max_valid_at, cur_valid_at)
                            # END for r in serv_response

                            await update_shard_info_stmt.executemany([(max_valid_at, shard_id)])
                        # END async with shard_locks[shard_id](Write)
                    # END for shard_id in shard_data
                # END async with conn.transaction()
            # END async with common.pool.acquire() as conn
        # END async with common.lock(Read)

        # Return the response payload
        return jsonify(ic({
            'message': f"{len(data)} data entries added",
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
