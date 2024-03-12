from quart import Blueprint, current_app, jsonify, request
from colorama import Fore, Style
import sys

from consts import *
from common import *

blueprint = Blueprint('update', __name__)

@blueprint.route('/update', methods=['POST'])
async def data_write():
    """
        Update data entries in the database

        Request payload:
            "shard" : <shard_id>
            "stud_id" : <stud_id>
            "data" : {"stud_id": <stud_id>, "stud_name": <stud_name>, "stud_marks": <stud_marks>}
            "valid_idx" : <valid_idx>

        Response payload:
            "message": Data entry for stud_id:<stud_id> updated
            "curr_idx": <curr_idx>
            "status": "success"
            
    """

    global term

    try:
        payload: dict = await request.get_json()

        valid_idx = int(payload.get('valid_idx', -1))

        # TBD: Apply rules, also increase the term if required

        shard_id = int(payload.get('shard', -1))
        data: dict = payload.get('data', [])
        

        # Insert the data into the database
        async with current_app.pool.acquire() as connection:
            async with connection.transaction():
                stmt = None
                

        # Send the response
        response_payload = {
            "message": f"Data entry for stud_id:{data['Stud_id']} updated",
            "curr_idx": term,
            "status": "success"
        }

        return jsonify(response_payload), 200
    
    except Exception as e:
        print(f'{Fore.RED}ERROR | '
                f'Error in data_write: {e}'
                f'{Style.RESET_ALL}',
                file=sys.stderr)
        return jsonify(ic(err_payload(e))), 400

                    


