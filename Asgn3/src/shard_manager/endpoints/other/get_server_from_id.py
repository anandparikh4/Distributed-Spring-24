from quart import Blueprint, jsonify, request

from utils import *

blueprint = Blueprint('get_server_from_id', __name__)


@blueprint.route('/get_server_from_id/<int:server_id>', methods=['GET'])
async def get_server_from_id(server_id: int):
    """
    Response Payload:
        server_id: int
        server: str
        shards: list[str]
    """

    global serv_ids
    global shard_map

    await asyncio.sleep(0)

    try:
        async with common.lock(Read):
            server = None
            for serv, serv_id in serv_ids.items():
                if serv_id == server_id:
                    server = serv
                    break

            if server is None:
                raise Exception('Server not found')

            shards = [shard
                      for shard, shard_map in shard_map.items()
                      if server in set(shard_map.getServerList())]

            payload = {
                'server_id': server_id,
                'server': server,
                'shards': shards,
            }

        return jsonify(ic(payload)), 200

    except Exception as e:
        return jsonify(ic(err_payload(e))), 400

# END get_server_from_id
