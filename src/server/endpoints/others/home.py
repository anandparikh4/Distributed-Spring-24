from quart import Blueprint, jsonify

from common import *
from consts import *

blueprint = Blueprint('home', __name__)

@blueprint.route('/home', methods=['GET'])
async def home():
    """
    Greet a client with its server ID.

    Response payload:
        message: Hello from Server: [ID]
        status: status of the response
    """

    return jsonify(ic({
        'message': f"Hello from Server: {SERVER_ID}",
        'status': "successful"
    })), 200