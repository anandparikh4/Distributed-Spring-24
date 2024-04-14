from quart import Blueprint, jsonify, request

from utils import *

blueprint = Blueprint('update', __name__)


@blueprint.route('/update', methods=['POST'])
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
        request_id: int,
        shard_id: str,
        json_payload: dict
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

        async with session.post(f'http://{server_name}:5000/update',
                                json=json_payload) as response:
            await response.read()

        return response
    # END update_put_wrapper

    try:
        # Convert the reponse to json object
        response_json = await request.get_json()

        if response_json is None:
            raise Exception('Payload is empty')

        # Convert the json response to dictionary
        payload = dict(response_json)
        ic(payload)

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

        if stud_id != data["stud_id"]:
            raise Exception("Cannot change stud_id field")

        async with common.pool.acquire() as conn:
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
                        (($1::INTEGER) < stud_id_low + shard_size)
                    FOR UPDATE;
                    ''',
                    stud_id
                )

                if record is None:
                    raise Exception(f'stud_id {stud_id} does not exist')

                shard_id: str = record["shard_id"]
                # new log to be inserted at valid_at + 1
                shard_valid_at: int = record["valid_at"] + 1

                # Convert to aiohttp request
                timeout = aiohttp.ClientTimeout(
                    connect=REQUEST_TIMEOUT)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    tasks = [asyncio.create_task(
                        update_put_wrapper(
                            session=session,
                            request_id=get_request_id(),
                            shard_id=shard_id,
                            json_payload={
                                "shard": shard_id,
                                "stud_id": stud_id,
                                "data": data,
                                "term": shard_valid_at
                            }
                        )
                    )]

                    serv_response = await asyncio.gather(*tasks, return_exceptions=True)
                    serv_response = [None
                                     if isinstance(r, BaseException)
                                     else r for r in serv_response]
                    serv_response = serv_response[0]
                # END async with aiohttp.ClientSession

                # If all replicas are not updated, then return an error
                if serv_response is None or serv_response.status != 200:
                    raise Exception('Failed to update data entry')

                await conn.execute(
                    '''--sql
                    UPDATE
                        ShardT
                    SET
                        valid_at = ($1::INTEGER)
                    WHERE
                        shard_id = ($2::TEXT)
                    ''',
                    shard_valid_at,
                    shard_id,
                )
            # END async with conn.transaction()
        # END async with common.pool.acquire() as conn

        # Return the response payload
        return jsonify(ic({
            'message': f"Data entry for stud_id: {stud_id} updated",
            'status': 'success'
        })), 200

    except Exception as e:

        return jsonify(ic(err_payload(e))), 400
    # END try-except
