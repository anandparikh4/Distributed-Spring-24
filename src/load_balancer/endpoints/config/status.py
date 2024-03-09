from quart import Blueprint, current_app, jsonify

from utils import *

blueprint = Blueprint('rep', __name__)


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
    global pool

    async with lock(Read):
        shards: List[Dict[str, Any]] = []

        async with pool.acquire() as conn:
            async with conn.transaction():
                stmt = await conn.prepare(
                    '''--sql
                        SELECT
                            stud_id_low,
                            shard_id,
                            shard_size,
                        FROM
                            shardT
                    ''')

                async for record in stmt.cursor():
                    shards.append(dict(record))
                # END async for record in stmt.cursor()
            # END async with conn.transaction()
        # END async with pool.acquire()

        servers_to_shards: Dict[str, List[str]] = {}

        for shard, servers in shard_map.items():
            for server in servers:
                servers_to_shards[server] = servers_to_shards.get(server, [])
                servers_to_shards[server].append(shard)
            # END for server in servers
        # END for shard, servers in shard_map.items()

        # Return the response payload
        return jsonify(ic({
            'N': len(replicas),
            'shards': shards,
            'servers': [{
                'id': serv_ids[server],
                'shards': shards,
            } for server, shards in servers_to_shards.items()],
        })), 200

    # END async with lock
# END status
