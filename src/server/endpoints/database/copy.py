from quart import Blueprint, jsonify, request
from colorama import Fore, Style
import sys

from rules import rules
from consts import *
from common import *

blueprint = Blueprint('copy', __name__)


@blueprint.route('/copy', methods=['GET'])
async def copy():
    """
        Returns all data entries corresponding to the requested shard tables in the server container

        Request payload:
            "shards": ["sh1", "sh2"...]
            "valid_at": <valid_at>

        Response payload:
            "sh1": [data]
            "sh2": [data]
            ...
            "status": "success"
            "valid_at": <valid_at>

        Error payload:
            "status": "error"
            "message": "error message"

    """

    try:
        # Get the shard ids
        payload: dict = await request.get_json()
        ic(payload)

        valid_at = int(payload.get('valid_at', -1))
        shards = list(payload.get('shards', []))

        response_payload = {}
        for shard in shards:
            response_payload[shard] = []

        # Get the data from the database
        async with pool.acquire() as connection:
            async with connection.transaction():

                tasks = [asyncio.create_task(rules(shard, valid_at))
                         for shard in shards]
                res = await asyncio.gather(*tasks, return_exceptions=True)

                if any(res):
                    raise Exception(f'Error in applying rules: {res}')

                # for shard in shards:
                #     await rules(shard, valid_at)

                async for record in connection.cursor(
                        '''--sql
                        SELECT Stud_id, Stud_name, Stud_marks
                        FROM StudT
                        WHERE shard_id = ANY($1::TEXT[])
                        AND created_at <= $2::INTEGER; 
                        ''',
                        shards, valid_at):

                    shard_id = record['shard_id']
                    record = dict(record)
                    response_payload[shard_id].append(record)

        response_payload['status'] = 'success'
        response_payload['valid_at'] = valid_at
        return jsonify(ic(response_payload)), 200

    except Exception as e:
        if DEBUG:
            print(f'{Fore.RED}ERROR | '
                  f'{e}'
                  f'{Style.RESET_ALL}',
                  file=sys.stderr)

        return jsonify(ic(err_payload(e))), 400
