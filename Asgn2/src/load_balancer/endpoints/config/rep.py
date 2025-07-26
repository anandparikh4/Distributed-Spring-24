from quart import Blueprint, jsonify

from utils import *

blueprint = Blueprint('rep', __name__)


@blueprint.route('/rep', methods=['GET'])
async def rep():
    """
    Return the number and list of replica hostnames.

    `Response payload:`
        `message:`
            `N: number of replicas`
            `replicas: list of replica hostnames`
        `status: status of the request`
    """

    global replicas

    async with common.lock(Read):

        # Return the response payload
        return jsonify(ic({
            'message': {
                'N': len(replicas),
                'replicas': replicas.getServerList(),
            },
            'status': 'successful',
        })), 200
    # END async with lock
# END rep
