from quart import Blueprint, current_app, jsonify, request
from colorama import Fore, Style
import sys

from consts import *
from common import *

blueprint = Blueprint('copy', __name__)

@blueprint.route('/copy', methods=['GET'])
async def copy():
    """
        Returns all data entries corresponding to the requested shard tables in the server container

        Request payload:
            "shards": ["sh1", "sh2"...]
            "admin": true/false
            "valid_idx": <valid_idx>
        
        Response payload:
            "sh1": [data]
            "sh2": [data]
            ...
            "status": "success"
            "valid_idx": <valid_idx>

        Error payload:
            "status": "error"
            "message": "error message"
            
    """
    global term

    try:
        # Get the shard ids
        payload = await request.get_json()
        ic(payload)

        valid_idx = int(payload.get('valid_idx', -1))

        # TBD: Apply rules

        shards: list = payload.get('shards', [])
        admin = False
        if payload.get('admin', '').lower() == 'true':
            admin = True

        response_payload = {}

        # Get the data from the database
        async with current_app.pool.acquire() as connection:
            async with connection.transaction():
                stmt = None
                if admin:       # If admin, return data with shard_id and modified_at (Timestamp)
                    stmt = connection.prepare('''
                        SELECT * FROM StudT WHERE shard_id = ANY($1::varchar[]);
                    ''')
                else:           # If not admin, return only data
                    stmt = connection.prepare('''
                        SELECT Stud_id, Stud_name, Stud_marks FROM StudT WHERE shard_id = ANY($1::varchar[]);
                    ''')

                async for record in stmt.cursor(shards):
                    shard_id = record['shard_id']
                    record = dict(record)
                    if shard_id in response_payload:
                        response_payload[shard_id].append(record)
                    else:
                        response_payload[shard_id] = [record]

        response_payload['status'] = 'success'
        response_payload['valid_idx'] = term
        return jsonify(ic(response_payload)), 200
    
    except Exception as e:
        if DEBUG:
            print(f'{Fore.RED}ERROR | '
                  f'{e}'
                  f'{Style.RESET_ALL}',
                  file=sys.stderr)
            
        return jsonify(ic(err_payload(e))), 400     # Need to add err_playload() [to common.py ?]

                    


