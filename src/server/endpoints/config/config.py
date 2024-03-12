from quart import Blueprint, current_app, jsonify, request
from colorama import Fore, Style
import sys

from consts import *
from common import *

blueprint = Blueprint('config', __name__)

@blueprint.route('/config', methods=["POST"])
async def server_config():
    """
        Assigns the list of shards whose data the server must store

        Request payload:
            "shard_list" : ["sh0" , "sh1" , "sh2"]
        
        Response payload:
            "status" : "success"

        Error payload:
            "status" : "error"
            "message" : "error message"
    """

    try:
        # Get the list of shards from payload
        payload: dict = await request.get_json()
        ic(payload)

        shard_list: list = payload.get("shard_list", [])

        # Add to the database
        response_payload = {}
        async with current_app.pool.acquire() as connection:
            async with connection.transaction():
                stmt = connection.prepare('''
                    INSERT INTO ShardList
                    SELECT * FROM UNNEST ($1);
                ''')
                await connection.execute(stmt , shard_list)
        
        response_payload['status'] = 'success'

        return jsonify(ic(response_payload)), 200
    
    except Exception as e:
        if DEBUG:
            print(f'{Fore.RED}ERROR | '
                  f'{e}'
                  f'{Style.RESET_ALL}',
                  file=sys.stderr)
        return jsonify(ic(err_payload(e))), 400
