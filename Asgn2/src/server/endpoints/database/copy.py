from quart import Blueprint, jsonify, request

import common
from common import *

from .rules import rules

blueprint = Blueprint('copy', __name__)


@blueprint.route('/copy', methods=['GET'])
async def copy():
    """
        Returns all data entries corresponding to the requested shard tables in the server container

        Request payload:
            "shards": ["sh1", "sh2"...]
            "valid_at": [<valid_at_sh1>, <valid_at_sh2>...]

        Response payload:
            "sh1": [data]
            "sh2": [data]
            ...
            "status": "success"

        Error payload:
            "status": "error"
            "message": "error message"

    """

    try:
        # Get the shard ids
        payload: dict = await request.get_json()
        ic(payload)

        shards: list[str] = list(payload.get('shards', []))
        valid_at: list[int] = list(payload.get('valid_at', -1))

        response_payload = {}
        for shard in shards:
            response_payload[shard] = []

        # Get the data from the database
        async with common.pool.acquire() as conn:
            async with conn.transaction():

                tasks = [asyncio.create_task(rules(shard, valid_at_shard))
                         for shard, valid_at_shard in zip(shards, valid_at)]

                res = await asyncio.gather(*tasks, return_exceptions=True)

                if any(res):
                    raise Exception(f'Error in applying rules: {res}')

                # for shard in shards:
                #     await rules(shard, valid_at)

                stmt = await conn.prepare(
                    '''--sql
                    SELECT Stud_id, Stud_name, Stud_marks
                    FROM StudT
                    WHERE shard_id = $1::TEXT
                        AND created_at <= $2::INTEGER;
                    ''')

                for shard, valid_at_shard in zip(shards, valid_at):
                    async for record in stmt.cursor(shard, valid_at_shard):
                        record = dict(record)
                        response_payload[shard].append(record)

        response_payload['status'] = 'success'

        return jsonify(ic(response_payload)), 200

    except Exception as e:

        return jsonify(ic(err_payload(e))), 400
