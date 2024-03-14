from quart import Blueprint, jsonify, request

import common
from common import *

blueprint = Blueprint('config', __name__)


@blueprint.route('/config', methods=["POST"])
async def server_config():
    """
        Assigns the list of shards whose data the server must store

        Request payload:
            "shards" : ["sh0" , "sh1" , "sh2" ...]

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

        shard_list: list = payload.get("shards", [])

        # Add to the database
        response_payload = {}
        async with common.pool.acquire() as connection:
            async with connection.transaction():
                stmt = await connection.prepare('''--sql
                    INSERT INTO TermT (shard_id)
                    VALUES ($1::TEXT);
                ''')

                await stmt.executemany((shard,) for shard in shard_list)

        response_payload['status'] = 'success'

        return jsonify(ic(response_payload)), 200

    except Exception as e:

        return jsonify(ic(err_payload(e))), 400
