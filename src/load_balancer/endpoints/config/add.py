from quart import Blueprint, jsonify, request

from utils import *

blueprint = Blueprint('add', __name__)


@blueprint.route('/add', methods=['POST'])
async def add():
    """
    Add new server replica hostname(s) and shards to the list.

    If `len(servers) == n`:
        Update the `serv_ids` with the new server ids.
        Update the `replicas` with the new hostnames.
        Update the `shard_map` with the new shard -> server mapping.
    If `new_shards` is not empty:
        Add the new shards to the `shard_map`.
        Add the new shards to the `shard_locks`.
        Add the new shards to the `shardT` table.

    If `len(servers) != n`:
        Return an error message.
    If relevant fields are not present in `new_shards`:
        Return an error message.
    If `n > remaining slots`:
        Return an error message.
    If `hostname` in `servers` already exists in `replicas`:
        Do not add any replicas to the list.
        Return an error message.
    If some shard in `new_shards` already exists in `shard_map`:
        Return an error message.
    If some shard for some server in `servers` does not exist in `shard_map` union `new_shards`:
        Return an error message.

    `Request payload:`
        `n: number of servers to add`
        `new_shards: list of new shard names and description to add [optional]`
            `stud_id_low: lower bound of student id`
            `shard_id: name of the shard`
            `shard_size: size of the shard`
        `servers: dict of server hostname -> list of shard names to add [new shard names must be define in `new_shards`]`

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
            async with session.post(f'http://Shard-Manager:5000/add',
                                   json=await request.get_json()) as response:
                return jsonify(ic(await response.json())), 200

    except Exception as e:
        return jsonify(ic(err_payload(e))), 400

# END add
