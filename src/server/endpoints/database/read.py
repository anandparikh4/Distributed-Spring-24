from quart import Blueprint, jsonify, request

from common import *

from .rules import rules

blueprint = Blueprint('read', __name__)


@blueprint.route('/read', methods=['GET'])
async def read():
    """
        Returns requested data entries from the server container

        Request payload:
            "shard"     : <shard_id>
            "stud_id"  : {"low": <low>, "high": <high>}
            "valid_at" : <valid_at>

        Response payload:
            "data" : [{"Stud_id": <low>, ...},
                      {"Stud_id": <low+1>, ...},
                      ...
                      {"Stud_id": <high>, ...}]
            "status": "success"
    """

    try:
        # Get the shard id and the range of stud_ids
        payload: dict = await request.get_json()
        ic(payload)

        valid_at = int(payload.get('valid_at', -1))
        shard_id = str(payload.get('shard', -1))
        stud_id = dict(payload.get('stud_id', {}))

        id_low = int(stud_id.get('low', -1))
        id_high = int(stud_id.get('high', -1))

        # Get the data from the database
        response_payload = {'data': [], 'status': 'success'}
        async with pool.acquire() as connection:
            async with connection.transaction():

                await rules(shard_id, valid_at)

                async for record in connection.cursor('''--sql
                    SELECT Stud_id, Stud_name, Stud_marks
                    FROM StudT
                    WHERE shard_id = $1
                        AND stud_id BETWEEN $2 AND $3
                        AND created_at <= $4;
                ''', shard_id, id_low, id_high, valid_at):

                    response_payload['data'].append(dict(record))

        return jsonify(ic(response_payload)), 200

    except Exception as e:
        if DEBUG:
            print(f'{Fore.RED}ERROR | '
                  f'{e}'
                  f'{Style.RESET_ALL}',
                  file=sys.stderr)
        return jsonify(ic(err_payload(e))), 400
