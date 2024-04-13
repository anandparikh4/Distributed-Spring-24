from quart import Blueprint, jsonify, request

from utils import *

blueprint = Blueprint('init', __name__)


@blueprint.route('/init', methods=['POST'])
async def init():
    """
    Initiaize the server replicas and shards.

    If `len(servers) == n`:
        Add `servers` to the list.
    If `shards` is not empty:
        Add the new shards to the `shard_map`.

    If `len(servers) != n`:
        Return an error message.
    If relevant fields are not present in `shards`:
        Return an error message.
    If `n > remaining slots`:
        Return an error message.
    If some shard for some server in `servers` does not exist in `shards`:
        Return an error message.

    `Request payload:`
        `n: number of servers to add`
        `shards: list of new shard names and description to add [optional]`
            `stud_id_low: lower bound of student id`
            `shard_id: name of the shard`
            `shard_size: size of the shard`
        `servers: dict of server hostname -> list of shard names to add [new shard names must be define in `shards`]`

    `Response payload:`
        `message:`
            `N: number of replicas`
            `replicas: list of replica servers`
        `status: status of the request`

    `Error payload:`
        `message: error message`
        `status: status of the request`
    """

    await asyncio.sleep(0)

    try:
        timeout = aiohttp.ClientTimeout(connect=REQUEST_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(f'http://Shard-Manager:5000/init',
                                   json=await request.get_json()) as response:
                return jsonify(ic(await response.json())), 200

    except Exception as e:
        return jsonify(ic(err_payload(e))), 400
# END add
