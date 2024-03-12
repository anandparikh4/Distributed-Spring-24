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
                async for record in stmt.cursor(low, high):
                    shard_ids.append(record["shard_id"])
                    shard_valid_ats.append(record["valid_at"])

        data = []

        async with lock(Read):
            for shard_id, shard_valid_at in zip(shard_ids, shard_valid_ats):
                if len(shard_map[shard_id]) > 0:
                    # TODO: Change to ConsistentHashMap
                    server_name = shard_map[shard_id][0]

                    async with shard_locks[shard_id](Read):
                        async def wrapper(
                            session: aiohttp.ClientSession,
                            server_name: str,
                            json_payload: Dict
                        ):

                            # To allow other tasks to run
                            await asyncio.sleep(0)

                            async with session.post(f'http://{server_name}:5000/read', json=json_payload) as response:
                                await response.read()

                            return response
                        # END wrapper

                         # Convert to aiohttp request
                        timeout = aiohttp.ClientTimeout(
                            connect=REQUEST_TIMEOUT)
                        async with aiohttp.ClientSession(timeout=timeout) as session:
                            tasks = [asyncio.create_task(wrapper(
                                session,
                                server_name,
                                json_payload={
                                    "shard": shard_id,
                                    "stud_id": stud_id,
                                    "valid_at": shard_valid_at
                                }
                            ))]
                            serv_response = await asyncio.gather(*tasks, return_exceptions=True)
                            serv_response = serv_response[0] if not isinstance(
                                serv_response[0], BaseException) else None
                        # END async with

                        if serv_response is None:
                            raise Exception('Server did not respond')

                        serv_response = dict(await serv_response.json())
                        data.extend(serv_response.get("data", []))
                    # END async with
                # END if
            # END for
        # END async with

        # Return the response payload
        return jsonify(ic({
            'shards_queried': shard_ids,
            'data': data,
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
