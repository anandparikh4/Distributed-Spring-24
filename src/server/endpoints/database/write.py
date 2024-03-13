from quart import Blueprint, jsonify, request

from common import *

from .rules import rules

blueprint = Blueprint('write', __name__)


@blueprint.route('/write', methods=['POST'])
async def write():
    """
        Write data entries to the database

        Request payload:
            "shard" : <shard_id>
            "data"  : [{"stud_id": <id1>, ...},
                      {"stud_id": <id2>, ...},
                      ...
                      {"stud_id": <idn>, ...}]
            "admin"    : true/false (optional)
            "valid_at" : <valid_at>

        Response payload:
            "message": Data entries added
            "valid_at": <valid_at>
            "status": "success"

    """

    try:
        payload: dict = await request.get_json()
        ic(payload)

        valid_at = int(payload.get('valid_at', -1))
        shard_id = str(payload.get('shard', -1))
        data = list(payload.get('data', []))

        admin = str(payload.get('admin', 'false')).lower() == 'true'

        # Insert the data into the database
        async with pool.acquire() as connection:
            async with connection.transaction():
                stmt = await connection.prepare('''--sql
                    INSERT INTO StudT (stud_id, stud_name, stud_marks, shard_id, created_at)
                    VALUES ($1::INTEGER, 
                            $2::TEXT, 
                            $3::INTEGER, 
                            $4::TEXT,
                            $5::INTEGER);
                    ''')

                if admin:
                    term = valid_at
                else:
                    term: int = await connection.fetchval('''--sql
                        SELECT term 
                        FROM TermT
                        WHERE shard_id = $1::TEXT;                         
                    ''', shard_id)

                    term = max(term, valid_at) + 1

                    await rules(shard_id, valid_at)

                await stmt.executemany([(record_dict['stud_id'],
                                         record_dict['stud_name'],
                                         record_dict['stud_marks'],
                                         shard_id,
                                         term)
                                        for record_dict in data])

                await connection.execute('''--sql
                    UPDATE TermT
                    SET term = $1::INTEGER
                    WHERE shard_id = $2::TEXT;
                ''', term, shard_id)

        # Send the response
        response_payload = {
            "message": "Data entries added",
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
