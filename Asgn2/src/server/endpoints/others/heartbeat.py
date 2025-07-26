from quart import Blueprint, Response

blueprint = Blueprint('heartbeat', __name__)


@blueprint.route('/heartbeat', methods=['GET'])
async def heartbeat():
    """
    Send heartbeat response upon request.

    Response payload:
        message: [EMPTY]
        status: status of the response
    """

    return Response(status=200)
