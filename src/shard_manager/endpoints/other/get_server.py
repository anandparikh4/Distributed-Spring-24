from quart import Blueprint, jsonify, request

from utils import *

blueprint = Blueprint('get_server', __name__)


@blueprint.route('/get_server', methods=['GET'])
async def get_server():

    await asyncio.sleep(0)

    request_json = await request.get_json()
    if request_json is None:
        raise Exception('Payload is empty')

    # Convert the json response to dictionary
    payload = dict(request_json)
    ic(payload)

    shard = payload.get('shard')
    request_id = payload.get('request_id')

    if shard is None:
        shard = "__all__"

    if request_id is None:
        raise Exception('`request_id` field is required')

    shard = str(shard)
    request_id = int(request_id)

    try:
        async with common.lock(Read):
            if shard == "__all__":
                server = replicas.find(request_id)
                payload = {'server': server}
            else:
                server = shard_map[shard].find(request_id)
                payload = {'shard': shard, 'server': server}

        return jsonify(ic(payload)), 200

    except Exception as e:
        return jsonify(ic(err_payload(e))), 400
# END get_server
