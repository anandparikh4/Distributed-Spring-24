from quart import Blueprint, jsonify, request
import common
from common import *
from .rules import bookkeeping

blueprint = Blueprint('copy', __name__)

@blueprint.route('/copy', methods=['GET'])
async def copy():
    """
        Returns all data entries and log entries corresponding to the requested shard tables in the server container

        Request payload:
            "shards": ["sh1", "sh2", ...]
            "terms": [<term1>, <term2>, ...]

        Response payload:
            "data"  : {"sh1": [data], "sh2": [data]}
            "log"   : {"sh1": [log], "sh2": [log]}
            "status": "success"

        Error payload:
            "status": "error"
            "message": "error message"
    """

    try:
        payload: dict = await request.get_json()
        ic(payload)

        # decode payload
        shards: list[str] = list(payload.get('shards', []))
        terms: list[int] = list(payload.get('terms', []))

        response_payload = {
            "data": {},
            "log": {}
        }
        for shard_id in shards:
            response_payload["data"][shard_id] = []
            response_payload["log"][shard_id] = []

        # perform bookkeeping on all shards, then get data from StudT and logs from LogT
        async with common.pool.acquire() as conn:
            async with conn.transaction():

                tasks = [asyncio.create_task(bookkeeping(shard_id, term, "r")) 
                        for shard_id, term in zip(shards, terms)]

                res = await asyncio.gather(*tasks, return_exceptions=True)

                if any(res):
                    raise Exception(f'Error in performing bookkeeping: {res}')
                
                stmt = await conn.prepare('''--sql
                    SELECT stud_id, stud_name, stud_marks
                    FROM StudT
                    WHERE shard_id = $1::TEXT
                    ''')

                for shard_id in shards:
                    async for record in stmt.cursor(shard_id):
                        record = dict(record)
                        response_payload["data"][shard_id].append(record)

                stmt = await conn.prepare('''--sql
                    SELECT log_idx, operation, stud_id, content
                    FROM LogT
                    WHERE shard_id = $1::TEXT
                    ''')
                
                for shard_id in shards:
                    async for log in stmt.cursor(shard_id):
                        log = dict(log)
                        response_payload["log"][shard_id].append(log)

        response_payload['status'] = 'success'
        return jsonify(ic(response_payload)), 200

    except Exception as e:
        return jsonify(ic(err_payload(e))), 400
