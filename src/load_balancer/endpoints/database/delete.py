from quart import Blueprint, current_app, jsonify, request

from common import *
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
        
        # Get the shard name containing the entry
        shard_id = None
        pool = current_app.pool
        async with pool.acquire() as conn:
            stmt = conn.prepare(
            '''
            SELECT shard_id FROM ShardT WHERE 
            (stud_id_low <= ($1::int)) AND (($1::int) <= stud_id_low + shard_size)
            ''')
            async with conn.transaction():
                async for record in stmt.cursor(stud_id):
                    shard_id = record["shard_id"]
        
        if not shard_id:
            raise Exception(f'stud_id {stud_id} does not exist')

        async with lock(Read):
            server_names = shard_map[shard_id] # TODO: Change to ConsistentHashMap
            async with shard_locks[shard_id](Read):
                async def wrapper(
                    session: aiohttp.ClientSession,
                    server_name: str,
                    json_payload: dict
                ):

                    # To allow other tasks to run
                    await asyncio.sleep(0)

                    async with session.put(f'http://{server_name}:5000/del', json=json_payload) as response:
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
                            "valid_at": valid_at
                        }
                    )) for server_name in server_names]
                    serv_response = await asyncio.gather(*tasks, return_exceptions=True)
                    serv_response = serv_response[0] if not isinstance(
                        serv_response[0], BaseException) else None
                # END async with

                if serv_response is None:
                    raise Exception('Server did not respond')
                
                serv_response = await serv_response.json()
                # TODO: Update valid_at from server_response
            # END async with
        # END async with
                    
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