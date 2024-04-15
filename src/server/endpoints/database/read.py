from quart import Blueprint, jsonify, request
import common
from common import *
from .rules import bookkeeping

blueprint = Blueprint('read', __name__)


@blueprint.route('/read', methods=['GET'])
async def read():
    """
        Returns requested data entries from the server container

        Request payload:
            "shard"     : <shard_id>
            "term"      : <term>
            "stud_id"   : {"low": <low>, "high": <high>}

        Response payload:
            "data"  : [{"Stud_id": <low>, ...},
                      {"Stud_id": <low+1>, ...},
                      ...
                      {"Stud_id": <high>, ...}]
            "status": "success"
    """

    try:
        payload: dict = await request.get_json()
        ic(payload)

        # decode arguments from payload
        shard_id = str(payload.get('shard', ""))
        term = int(payload.get('term', -1))
        stud_id = dict(payload.get('stud_id', {}))
        id_low = int(stud_id.get('low', -1))
        id_high = int(stud_id.get('high', -1))

        # perform bookkeeping
        await bookkeeping(shard_id, term, "r")

        # get data from StudT
        response_payload = {'data': [], 'status': 'success'}
        async with common.pool.acquire() as conn:
            async with conn.transaction():

                async for record in conn.cursor('''--sql
                    SELECT stud_id, stud_name, stud_marks
                    FROM StudT
                    WHERE shard_id = $1::TEXT
                    AND stud_id BETWEEN $2::INTEGER AND $3::INTEGER;
                ''', shard_id, id_low, id_high):

                    response_payload['data'].append(dict(record))

        return jsonify(ic(response_payload)), 200

    except Exception as e:
        return jsonify(ic(err_payload(e))), 400
