from quart import Blueprint, current_app, jsonify, request

from utils import *

blueprint = Blueprint('rm', __name__)


@blueprint.route('/rm', methods=['DELETE'])
async def rm():
    """
    Delete server replica hostname(s) from the list.

    If `len(hostnames) <= n`:
        Delete `hostnames` and `n - len(hostnames)` random hostnames from the list.
        While selecting other random hostnames, only select those servers which are not the only server for any shard.

    If `n <= 0`:
        Return an error message.
    If `n > len(replicas)`:
        Return an error message.
    If `len(hostnames) > n`:
        Return an error message.
    If for any hostname in `hostnames`, `hostname not in replicas`:
        Do not delete any replicas from the list.
        Return an error message.
    If for any hostname S in `hostnames`, there exists some shard K such that S is the only server in `shard_map[K]`:
        Do not delete any replicas from the list.
        Return an error message.

    Random hostnames are deleted from the list of replicas.

    `Request payload:`
        `n: number of servers to delete`
        `hostnames: list of server replica hostnames to delete (<= n) [optional]`

    `Response payload:`
        `message:`
            `N: number of replicas`
            `replicas: list of replica hostnames`
        `status: status of the request`

    `Error payload:`
        `message: error message`
        `status: status of the request`
    """

    await asyncio.sleep(0)

    try:
        timeout = aiohttp.ClientTimeout(connect=REQUEST_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.delete(f'http://Shard-Manager:5000/rm',
                                   json=await request.get_json()) as response:
                return (await response.content.read(),
                        response.status,
                        dict(response.headers))

    except Exception as e:
        return jsonify(ic(err_payload(e))), 400
# END rm
