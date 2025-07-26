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
        request_id: int,
        shard_id: str,
        json_payload: Dict
    ):
        # To allow other tasks to run
        await asyncio.sleep(0)

        async with session.get(f'http://Shard-Manager:5000/get_primary',
                               json={'request_id': request_id,
                                     'shard': shard_id}) as response:
            await response.read()

        server_info = await response.json()
        server_name = server_info.get('primary')
        secondary = server_info.get('secondary')

        json_payload["is_primary"] = True
        json_payload["secondary_servers"] = secondary

        async with session.post(f'http://{server_name}:5000/write',
                                json=json_payload) as response:
            await response.read()

        return response
    # END write_post_wrapper

    try:
        # Convert the reponse to json object
        response_json = await request.get_json()

        if response_json is None:
            raise Exception('Payload is empty')

        # Convert the json response to dictionary
        payload = dict(response_json)
        ic(payload)

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
        # [shard_id] -> (list of entries, valid_at)
        shard_data: Dict[str, Tuple[List[Dict[str, Any]], int]] = {}

        async with common.pool.acquire() as conn:
            async with conn.transaction():
                get_shard_id_stmt = await conn.prepare(
                    '''--sql
                    SELECT
                        shard_id
                    FROM
                        ShardT
                    WHERE
                        (stud_id_low <= ($1::INTEGER)) AND
                        (($1::INTEGER) < stud_id_low + shard_size);
                    ''')

                get_valid_at_stmt = await conn.prepare(
                    '''--sql
                    SELECT
                        valid_at
                    FROM
                        ShardT
                    WHERE
                        shard_id = $1::TEXT
                    FOR UPDATE;
                    ''')

                update_shard_info_stmt = await conn.prepare(
                    '''--sql
                    UPDATE
                        ShardT
                    SET
                        valid_at = ($1::INTEGER)
                    WHERE
                        shard_id = ($2::TEXT);
                    ''')

                for entry in data:
                    stud_id = int(entry["stud_id"])
                    record = await get_shard_id_stmt.fetchrow(stud_id)

                    if record is None:
                        raise Exception(
                            f'Shard for {stud_id = } does not exist')

                    shard_id: str = record["shard_id"]

                    if shard_id not in shard_data:
                        shard_data[shard_id] = ([], 0)

                    shard_data[shard_id][0].append(entry)
                # END for entry in data

                # To prevent deadlocks in the database, sort the shard_ids
                for shard_id in sorted(shard_data.keys()):
                    shard_valid_at: int = await get_valid_at_stmt.fetchval(shard_id) + 1
                    shard_data[shard_id] = (shard_data[shard_id][0],
                                            shard_valid_at)

                timeout = aiohttp.ClientTimeout(connect=REQUEST_TIMEOUT)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    for shard_id in shard_data:
                        # Convert to aiohttp request
                        tasks = [asyncio.create_task(
                            write_post_wrapper(
                                session=session,
                                request_id=get_request_id(),
                                shard_id=shard_id,
                                json_payload={
                                    "shard": shard_id,
                                    "data": shard_data[shard_id][0],
                                    "term": shard_data[shard_id][1],
                                }
                            )
                        )]

                        serv_response = await asyncio.gather(*tasks, return_exceptions=True)
                        serv_response = [None if isinstance(r, BaseException)
                                         else r for r in serv_response]
                        serv_response = serv_response[0]

                        # If all replicas are not updated, then return an error
                        if serv_response is None or serv_response.status != 200:
                            raise Exception('Failed to write all data entries')

                        await update_shard_info_stmt.executemany(
                            [(shard_data[shard_id][1], shard_id)])
                    # END for shard_id in shard_data
                # END async with aiohttp.ClientSession
            # END async with conn.transaction()
        # END async with common.pool.acquire() as conn

        # Return the response payload
        return jsonify(ic({
            'message': f"{len(data)} data entries added",
            'status': 'success'
        })), 200

    except Exception as e:

        return jsonify(ic(err_payload(e))), 400
    # END try-except
