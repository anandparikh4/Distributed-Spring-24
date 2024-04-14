from quart import Blueprint, jsonify, request
import common
from common import *

blueprint = Blueprint('copy', __name__)


@blueprint.route('/copy', methods=['GET'])
async def copy():
    """
        Copy entire StudT, LogT and accordingly update TermT

        Request payload:
            "data"  : {"sh1": [data], "sh2": [data], ...}
            "log"   : {"sh1": [log], "sh2": [log], ...}
            "term"  : {"sh1": <term>, "sh2": <term>, ...}

        Response payload:
            "status": "success"
            "message": "success message"

        Error payload:
            "status": "error"
            "message": "error message"
    """

    try:
        payload: dict = await request.get_json()
        ic(payload)

        # decode payload
        payload_data: dict = dict(payload.get("data", {}))
        payload_log: dict = dict(payload.get("log", {}))
        payload_term: dict = dict(payload.get("term", {}))

        all_data = []
        for shard_id, records in payload_data.items():
            for record in records:
                all_data.append(
                    (record["stud_id"], record["stud_name"],
                     record["stud_marks"], shard_id))

        all_logs = []
        for shard_id, logs in payload_log.items():
            for log in logs:
                all_logs.append(
                    (log["log_idx"], shard_id,
                     log["operation"], log["stud_id"],
                     log["content"]))

        # copy full StudT and LogT, update TermT accordingly
        async with common.pool.acquire() as conn:
            async with conn.transaction():
                # copy StudT
                stmt = await conn.prepare('''--sql
                    INSERT INTO StudT
                    VALUES ($1::INTEGER,
                            $2::TEXT,
                            $3::INTEGER,
                            $4::TEXT);
                    ''')
                await stmt.executemany(all_data)

                # copy LogT
                stmt = await conn.prepare('''--sql
                    INSERT INTO LogT
                    VALUES ($1::INTEGER,
                            $2::TEXT,
                            $3::TEXT,
                            $4::INTEGER,
                            $5::JSON);                
                    ''')
                await stmt.executemany(all_logs)

                # update TermT
                # all_terms = []
                # async for log in conn.cursor('''--sql
                #     SELECT shard_id , MAX(log_idx) as last_idx
                #     FROM LogT
                #     GROUP BY shard_id
                # '''):
                #     all_terms.append(
                #         (log["shard_id"], int(log["last_idx"], True)))

                stmt = await conn.prepare('''--sql
                    INSERT INTO TermT
                    VALUES ($1::TEXT,
                            $2::INTEGER,
                            $3::BOOLEAN);
                    ''')
                await stmt.executemany([
                    (shard_id, term, True)
                    for shard_id, term in payload_term.items()
                ])

        response_payload = {
            "status": 200,
            "message": "Recovered successfully"
        }
        return jsonify(ic(response_payload)), 200

    except Exception as e:
        return jsonify(ic(err_payload(e))), 400
