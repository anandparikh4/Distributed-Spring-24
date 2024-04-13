from quart import Blueprint, jsonify

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

    await asyncio.sleep(0)

    try:
        timeout = aiohttp.ClientTimeout(connect=REQUEST_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # Get the list of replicas
            async with session.get(f'http://Shard-Manager:5000/status') as response:
                return jsonify(ic(await response.json())), 200

    except Exception as e:
        return jsonify(ic(err_payload(e))), 400
    # END try-except
# END status
