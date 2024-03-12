from quart import Blueprint, current_app, jsonify, request

from common import *
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

    try:
        # Get the request payload
        payload: dict = await request.get_json()
        ic(payload)

        if payload is None:
            raise Exception('Payload is empty')
        
        # Get the required fields from the payload and check for errors
        stud_id: dict = payload.get('stud_id')

        if stud_id is None:
            raise Exception('Payload does not contain `stud_id` field')
        
        data: dict = payload.get('data')

        if data is None:
            raise Exception('Payload does not contain `data` field')
        
        stud_id = int(data.get("stud_id", -1))
        stud_name = str(data.get("stud_name", ""))
        stud_marks = int(data.get("stud_marks", -1))
        if (stud_id == -1 or stud_name == "" or stud_marks == -1):
            raise Exception(f'Data is invalid')
        
        # Get the shard name and valid at containing the entry
        shard_id, shard_valid_at = None, None
        pool = current_app.pool
        async with pool.acquire() as conn:
            stmt = conn.prepare(
            '''
            SELECT shard_id, valid_at FROM ShardT WHERE 
            (stud_id_low <= ($1::int)) AND (($1::int) <= stud_id_low + shard_size)
            ''')
            async with conn.transaction():
                async for record in stmt.cursor(stud_id):
                    shard_id = record["shard_id"]
                    shard_valid_at = record["valid_at"]
        
        if not shard_id:
            raise Exception(f'stud_id {stud_id} does not exist')

        async with lock(Read):
            pool = current_app.pool
            async with pool.acquire() as conn:
                stmt = conn.prepate(
                    '''
                    UPDATE TABLE ShardT
                    SET valid_at=($2::int) 
                    WHERE shard_id=($1:int)
                    '''
                )
                server_names = shard_map[shard_id] # TODO: Change to ConsistentHashMap
                max_valid_at = shard_valid_at
                async with shard_locks[shard_id](Read):
                    async def wrapper(
                        session: aiohttp.ClientSession,
                        server_name: str,
                        json_payload: dict
                    ):

                        # To allow other tasks to run
                        await asyncio.sleep(0)

                        async with session.put(f'http://{server_name}:5000/update', json=json_payload) as response:
                            await response.read()

                            return response
                        # END wrapper

                    # Convert to aiohttp request
                    timeout = aiohttp.ClientTimeout(connect=REQUEST_TIMEOUT)
                    async with aiohttp.ClientSession(timeout=timeout) as session:
                        tasks = [asyncio.create_task(wrapper(
                            session, 
                            server_name, 
                            json_payload={
                                "shard": shard_id,
                                "stud_id": stud_id,
                                "data": data,
                                "valid_at": shard_valid_at
                            }
                        )) for server_name in server_names]
                        serv_response = await asyncio.gather(*tasks, return_exceptions=True)
                        serv_response = serv_response[0] if not isinstance(
                            serv_response[0], BaseException) else None
                    # END async with

                    if serv_response is None:
                        raise Exception('Server did not respond')

                    serv_response: dict = await serv_response.json()
                    cur_valid_at = serv_response.get("valid_at", -1)
                    if cur_valid_at == -1:
                        raise Exception('Server response did not contain valid_at field')
                    max_valid_at = max(max_valid_at, cur_valid_at)
                # END async with
                stmt.execute(shard_id, max_valid_at)
            # END async with
        # END async with
                    
        # Return the response payload
        return jsonify(ic({
            'message': f"Data entry for stud_id: {stud_id} updated",
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