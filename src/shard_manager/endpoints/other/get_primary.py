from quart import Blueprint, jsonify, request

from utils import *

blueprint = Blueprint('get_primary', __name__)


@blueprint.route('/get_primary', methods=['GET'])
async def get_primary():
    """
    `Request Payload`
        `shard`: str
        `request_id`: int

    `Response Payload`
        `shard`: str
        `primary`: str
    """

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
        raise Exception('`shard` field is required')

    if request_id is None:
        raise Exception('`request_id` field is required')

    shard = str(shard)
    request_id = int(request_id)

    try:
        async with common.lock(Read):
            primary = shard_map[shard].find(request_id)

        return jsonify(ic({
            'shard': shard,
            'primary': primary
        })), 200

    except Exception as e:
        return jsonify(ic(err_payload(e))), 400
# END get_primary
