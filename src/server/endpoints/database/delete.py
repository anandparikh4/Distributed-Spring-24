from quart import Blueprint, jsonify, request

import common
from common import *

from .rules import rules

blueprint = Blueprint('delete', __name__)


@blueprint.route('/del', methods=['DELETE'])
async def delete():
    """
        Delete data entries from the database

        Request payload:
            "shard"     : <shard_id>
            "stud_id"   : <stud_id>
            "valid_at"  : <valid_at>

        Response payload:
            "message"  : Data entry with stud_id:<stud_id> removed
            "status"   : "success"
            "valid_at" : <valid_at>

    """

    try:
        # Get the request payload
        payload: dict = await request.get_json()
        ic(payload)

        valid_at = int(payload.get('valid_at', -1))
        shard_id = str(payload.get('shard', -1))
        stud_id = int(payload.get('stud_id', -1))

        # Delete data from the database
        async with common.pool.acquire() as conn:
            async with conn.transaction():
                # Apply rules
                await rules(shard_id, valid_at)

                # Check if stud_id exists
                row = await conn.fetchrow('''--sql
                    SELECT *
                    FROM StudT
                    WHERE stud_id = $1::INTEGER;
                ''', stud_id)

                if row is not None:
                    raise Exception(f"Failed to delete")

                # Get the term
                term: int = await conn.fetchval('''--sql
                    SELECT term
                    FROM TermT
                    WHERE shard_id = $1::TEXT;                         
                ''', shard_id)

                # Increment term
                term = max(term, valid_at) + 1

                # Update the deleted_at field
                await conn.execute('''--sql
                    UPDATE StudT
                    SET deleted_at = $1::INTEGER
                    WHERE stud_id = $2::INTEGER 
                        AND shard_id = $3::TEXT
                        AND created_at <= $4::INTEGER;
                ''', term, stud_id, shard_id, valid_at)

                # Save the term in the TermT table
                await conn.execute('''--sql
                    UPDATE TermT
                    SET term = $1::INTEGER
                    WHERE shard_id = $2::TEXT;
                ''', term, shard_id)
                

        response_payload = {
            "message": f'Data entry with Stud_id:{stud_id} removed',
            "status": "success",
            "valid_at": term
        }

        return jsonify(ic(response_payload)), 200

    except Exception as e:
        print(f'{Fore.RED}ERROR | '
              f'Error in data_write: {e}'
              f'{Style.RESET_ALL}',
              file=sys.stderr)
        return jsonify(ic(err_payload(e))), 400
