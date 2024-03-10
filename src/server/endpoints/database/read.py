from quart import Blueprint, current_app, jsonify, request
from colorama import Fore, Style
import sys

from consts import *
from common import *

blueprint = Blueprint('read', __name__)

@blueprint.route('/read', methods=['GET'])
async def data_read():
    """
        Returns requested data entries from the server container

        Request payload:
            "shard_id" : "sh1"
            "stud_id"  : {"low": low, "high": high}

        Response payload:
            "data" : [{"stud_id": low, ...},
                      {"stud_id": low+1, ...},
                      ...
                      {"stud_id": high, ...}]
            "status": "success"
    """

    try:
        # Get the shard id and the range of stud_ids
        payload: dict = await request.get_json()
        ic(payload)

        shard_id = payload.get('shard_id', -1)
        stud_id : dict = payload.get('stud_id', {})

        id_low = stud_id.get('low', -1)
        id_high = stud_id.get('high', -1)

        # Get the data from the database
        response_payload = {}
        async with current_app.pool.acquire() as connection:
            async with connection.transaction():
                stmt = connection.prepare('''
                    SELECT stud_id, stud_name, stud_marks FROM StudT
                    WHERE shard_id = $1
                    AND stud_id BETWEEN $2 AND $3;
                ''')
                response_payload['data'] = []
                async for record in stmt.cursor(shard_id, id_low, id_high):
                    response_payload['data'].append(dict(record))

        response_payload['status'] = 'success'

        return jsonify(ic(response_payload)), 200
    
    except Exception as e:
        if DEBUG:
            print(f'{Fore.RED}ERROR | '
                  f'{e}'
                  f'{Style.RESET_ALL}',
                  file=sys.stderr)
        return jsonify(ic(err_payload(e))), 400

