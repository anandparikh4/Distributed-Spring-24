from quart import Blueprint, current_app, jsonify

from utils import *

blueprint = Blueprint('status', __name__)


@blueprint.route('/status', methods=['GET'])
async def status():
    """
    Return the number and list of replica hostnames.

    `Response payload:`
        `message:`
            `N: number of replicas`
            `replicas: list of replica hostnames`
        `status: status of the request`
    """

    global replicas
    global shard_map
    global serv_ids

    await asyncio.sleep(0)

    try:
        async with common.lock(Read):
            shards: List[Dict[str, Any]] = []

            async with common.pool.acquire() as conn:
                async with conn.transaction(readonly=True):
                    stmt = await conn.prepare(
                        '''--sql
                            SELECT
                                stud_id_low,
                                shard_id,
                                shard_size
                            FROM
                                shardT;
                        ''')

                    async for record in stmt.cursor():
                        shards.append(dict(record))
                    # END async for record in stmt.cursor()
                # END async with conn.transaction()
            # END async with common.pool.acquire()

            for shard_info in shards:
                shard_info['primary'] = shard_primary[shard_info['shard_id']]

            servers_to_shards: Dict[str, List[str]] = {}
            _shard_map = {k: v.getServerList() for k, v in shard_map.items()}

            ic(_shard_map)

            for shard, servers in _shard_map.items():
                for server in servers:
                    if server not in servers_to_shards.keys():
                        servers_to_shards[server] = []
                    servers_to_shards[server].append(shard)
                # END for server in servers
            # END for shard, servers in shard_map.items()

            ic(servers_to_shards)

            # Return the response payload
            return jsonify(ic({
                'N': len(replicas),
                'shards': shards,
                'servers': {
                    server: {
                        'id': serv_ids[server],
                        'shards': shards
                    } for server, shards in servers_to_shards.items()},
            })), 200

        # END async with lock
    except Exception as e:

        return jsonify(ic(err_payload(e))), 400
    # END try-except
# END status
