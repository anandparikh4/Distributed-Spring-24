from quart import Blueprint, jsonify, request

from common import *

from .rules import rules

blueprint = Blueprint('update', __name__)


@blueprint.route('/update', methods=['POST'])
async def data_write():
    """
        Update data entries in the database

        Request payload:
            "shard" : <shard_id>
            "stud_id" : <stud_id>
            "data" : {"stud_id": <stud_id>, "stud_name": <stud_name>, "stud_marks": <stud_marks>}
            "valid_at" : <valid_at>

        Response payload:
            "message": Data entry for stud_id:<stud_id> updated
            "valid_at": <valid_at>
            "status": "success"

    """

    try:
        payload: dict = await request.get_json()
        ic(payload)

        valid_at = int(payload.get('valid_at', -1))
        shard_id = str(payload.get('shard', -1))
        data = dict(payload.get('data', {}))

        # Insert the data into the database
        async with pool.acquire() as connection:
            async with connection.transaction():
                term: int = await connection.fetchval('''--sql
                    SELECT term FROM TermT
                    WHERE shard_id = $1::TEXT;                         
                ''', shard_id)

                term = max(term, valid_at) + 1

                await rules(shard_id, valid_at)

                await connection.execute('''--sql
                    UPDATE StudT
                    SET deleted_at = $1::INTEGER
                    WHERE stud_id = $2::INTEGER
                    AND created_at <= $3::INTEGER
                    AND shard_id = $4::TEXT;                  
                ''', term, data['stud_id'], valid_at, shard_id)

                term += 1

                await connection.execute('''--sql
                    INSERT INTO StudT (stud_id, stud_name, stud_marks, shard_id, created_at)
                    VALUES ($1::INTEGER, 
                            $2::TEXT, 
                            $3::INTEGER, 
                            $4::TEXT, 
                            $5::INTEGER);               
                ''', data['stud_id'], data['stud_name'], data['stud_marks'], shard_id, term)

                await connection.execute('''--sql
                    UPDATE TermT
                    SET term = $1::INTEGER
                    WHERE shard_id = $2::TEXT;                    
                ''', term, shard_id)

        # Send the response
        response_payload = {
            "message": f"Data entry for stud_id:{data['stud_id']} updated",
            "valid_at": term,
            "status": "success"
        }

        return jsonify(ic(response_payload)), 200

    except Exception as e:
        print(f'{Fore.RED}ERROR | '
              f'Error in data_write: {e}'
              f'{Style.RESET_ALL}',
              file=sys.stderr)
        return jsonify(ic(err_payload(e))), 400
