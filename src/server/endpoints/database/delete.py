from quart import Blueprint, current_app, jsonify, request
from colorama import Fore, Style
import sys

from consts import *
from common import *

blueprint = Blueprint('delete', __name__)

@blueprint.route('/delete', methods=['DELETE'])
async def delete():
    """
        Delete data entries from the database

        Request payload:
            "shard"     : <shard_id>
            "stud_id"   : <stud_id>
            "valid_idx" : <valid_idx>

        Response payload:
            "message"  : Data entry with stud_id:<stud_id> removed
            "status"   : "success"
            "valid_idx" : <valid_idx>
            
    """

    global term

    try:
        payload: dict = await request.get_json()

        valid_idx = int(payload.get('valid_idx', -1))

        # TBD: Apply rules, also increase the term if required

        shard_id = int(payload.get('shard', -1))
        stud_id = int(payload.get('stud_id', -1))

        # Delete data from the database
        async with current_app.pool.acquire() as connection:
            async with connection.transaction():
                stmt = connection.prepare('''
                    UPDATE StudT
                    SET deleted_at = $1
                    WHERE Stud_id = $2 
                    AND shard_id = $3;
                ''')
                await stmt.execute(term, stud_id, shard_id)

        # Send the response
        response_payload = {
            "message": f'Data entry with Stud_id:{stud_id} removed',
            "status": "success",
            "valid_idx": term
        }

        return jsonify(response_payload), 200
    
    except Exception as e:
        print(f'{Fore.RED}ERROR | '
                f'Error in data_write: {e}'
                f'{Style.RESET_ALL}',
                file=sys.stderr)
        return jsonify(ic(err_payload(e))), 400

                    


