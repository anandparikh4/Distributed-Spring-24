from quart import Blueprint, jsonify, request

from utils import *

blueprint = Blueprint('read', __name__)


@blueprint.route('/read', methods=['GET'])
async def read():
    """
    Read data entries from shard replicas across all server containers.

    If `low` > `high`:
        Return an error message.

    `Request payload:`
        `stud_id: dict for range of student ids`
            `low: lower limit of student id`
            `high: upper limit of student id`

    `Response payload:`
        `shards_queried: list of shard names containing the required entries`
        `data: list of entries having stundent ids within the given range`
            `stud_id: student id`
            `stud_name: student name`
            `stud_marks: student marks`
        `status: status of the request`

    `Error payload:`
        `message: error message`
        `status: status of the request`
    """

    await asyncio.sleep(0)

    async def read_get_wrapper(
        session: aiohttp.ClientSession,
        server_name: str,
        json_payload: Dict
    ):

        # To allow other tasks to run
        await asyncio.sleep(0)

        async with session.get(f'http://{server_name}:5000/read',
                               json=json_payload) as response:
            await response.read()

        return response
    # END read_get_wrapper

    try:
        # Get the request payload
        payload = dict(await request.get_json())
        ic(payload)

        if payload is None:
            raise Exception('Payload is empty')

        # Get the required fields from the payload
        stud_id = dict(payload.get('stud_id', {}))

        if len(stud_id) == 0:
            raise Exception('Payload does not contain `stud_id` field')

        # Check if stud_id contains the low and high fields and low <= high
        low = int(stud_id.get("low", -1))
        high = int(stud_id.get("high", -1))

        if low == -1:
            raise Exception('`stud_id` does not contain `low` field')

        if high == -1:
            raise Exception('`stud_id` does not contain `high` field')

        if low > high:
            raise Exception('`low` cannot be greater than `high`')

        # Get the shard names and valid ats containing the entries
        shard_ids: list[str] = []
        shard_valid_ats: list[int] = []

        async with common.lock(Read):
            async with common.pool.acquire() as conn:
                async with conn.transaction():
                    async for record in conn.cursor(
                        '''--sql
                        SELECT
                            shard_id,
                            valid_at
                        FROM
                            ShardT
                        WHERE
                            (stud_id_low <= ($2::INTEGER)) AND
                            (($1::INTEGER) < stud_id_low + shard_size)
                        FOR SHARE;
                        ''',
                            low, high):

                        shard_ids.append(record["shard_id"])
                        shard_valid_ats.append(record["valid_at"])
                    # END async for record in conn.cursor

                    if len(shard_ids) == 0:
                        raise Exception('No data entries found')

                    data = []

                    for shard_id, shard_valid_at in zip(shard_ids, shard_valid_ats):
                        if len(shard_map[shard_id]) == 0:
                            continue

                        # TODO: Change to ConsistentHashMap
                        server_name = shard_map[shard_id].find(
                            get_request_id())

                        # Convert to aiohttp request
                        timeout = aiohttp.ClientTimeout(
                            connect=REQUEST_TIMEOUT)
                        async with aiohttp.ClientSession(timeout=timeout) as session:
                            task = asyncio.create_task(
                                read_get_wrapper(
                                    session=session,
                                    server_name=server_name,
                                    json_payload={
                                        "shard": shard_id,
                                        "stud_id": stud_id,
                                        "valid_at": shard_valid_at
                                    }
                                )
                            )
                            serv_response = await asyncio.gather(*[task], return_exceptions=True)
                            serv_response = serv_response[0] if not isinstance(
                                serv_response[0], BaseException) else None
                        # END async with aiohttp.ClientSession(timeout=timeout) as session

                        if serv_response is None or serv_response.status != 200:
                            raise Exception('Failed to read data entry')

                        serv_response = dict(await serv_response.json())
                        data.extend(serv_response["data"])
                    # END for shard_id, shard_valid_at in zip(shard_ids, shard_valid_ats)
                # END async with conn.transaction()
            # END async with common.pool.acquire()
        # END async with common.lock(Read)

        # Return the response payload
        return jsonify(ic({
            'shards_queried': shard_ids,
            'data': data,
            'status': 'success'
        })), 200

    except Exception as e:

        return jsonify(ic(err_payload(e))), 400
    # END try-except
