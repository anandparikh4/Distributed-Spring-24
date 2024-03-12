from quart import Blueprint, current_app, jsonify, request

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

    try:
        # Get the request payload
        payload = dict(await request.get_json())
        ic(payload)

        if payload is None:
            raise Exception('Payload is empty')

        # Get the required fields from the payload
        data = list(payload.get('data', []))

        if len(data) == 0:
            raise Exception('Payload does not contain `data` field')

        # Check if the data entries are valid and compute the low and high ids
        for entry in data:
            stud_id = int(entry.get("stud_id", -1))
            stud_name = str(entry.get("stud_name", ""))
            stud_marks = int(entry.get("stud_marks", -1))
            if (stud_id == -1 or stud_name == "" or stud_marks == -1):
                raise Exception(f'Data entry "{entry}" is invalid')

        # Get the shard names and valid ats and the corresponding entries to be added
        # [shard_at] -> (list of entries, valid_at)
        shard_data: Dict[str, Tuple[List[Dict[str, Any]], int]] = {}

        async with pool.acquire() as conn:
            stmt = await conn.prepare(
                '''--sql
                SELECT
                    shard_id,
                    valid_at
                FROM
                    ShardT
                WHERE
                    (stud_id_low <= ($2::INTEGER)) AND
                    (($1::INTEGER) <= stud_id_low + shard_size)
                ''')

            async with conn.transaction():
                for entry in data:

                    stud_id = int(entry["stud_id"])
                    record = await stmt.fetchrow(stud_id)

                    if record is None:
                        raise Exception(f'stud_id {stud_id} does not exist')

                    shard_id: str = record["shard_id"]
                    shard_valid_at: int = record["valid_at"]

                    if shard_id not in shard_data:
                        shard_data[shard_id] = ([], shard_valid_at)

                    shard_data[shard_id][0].append(entry)

        async with lock(Read):
            async with pool.acquire() as conn:
                stmt = await conn.prepare(
                    '''--sql
                    UPDATE
                        ShardT
                    SET
                        valid_at = ($2::INTEGER)
                    WHERE
                        shard_id = ($1::INTEGER)
                    ''')

                async with conn.transaction():
                    for shard_id in shard_data:
                        # TODO: Chage to ConsistentHashMap
                        server_names = shard_map[shard_id]

                        max_valid_at = shard_data[shard_id][1]
                        async with shard_locks[shard_id](Write):
                            async def wrapper(
                                session: aiohttp.ClientSession,
                                server_name: str,
                                json_payload: Dict
                            ):
                                # To allow other tasks to run
                                await asyncio.sleep(0)

                                async with session.post(f'http://{server_name}:5000/write', json=json_payload) as response:
                                    await response.read()

                                return response
                            # END wrapper

                             # Convert to aiohttp request
                            timeout = aiohttp.ClientTimeout(
                                connect=REQUEST_TIMEOUT)
                            async with aiohttp.ClientSession(timeout=timeout) as session:
                                tasks = [asyncio.create_task(
                                    wrapper(
                                        session,
                                        server_name,
                                        json_payload={
                                            "shard": shard_id,
                                            "data": shard_data[shard_id][0],
                                            "valid_at": shard_data[shard_id][1]
                                        }
                                    )) for server_name in server_names]
                                serv_response = await asyncio.gather(*tasks, return_exceptions=True)
                                serv_response = serv_response[0] if not isinstance(
                                    serv_response[0], BaseException) else None
                            # END async with

                            if serv_response is None:
                                raise Exception('Server did not respond')

                            serv_response = dict(await serv_response.json())
                            cur_valid_at = int(
                                serv_response.get("valid_at", -1))

                            if cur_valid_at == -1:
                                raise Exception(
                                    'Server response did not contain valid_at field')
                            max_valid_at = max(max_valid_at, cur_valid_at)
                        # END async with
                        await stmt.executemany([(shard_id, max_valid_at)])
                    # END for
                # END async with
            # END async with
        # END async with

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
