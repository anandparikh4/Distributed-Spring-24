from quart import Blueprint, jsonify

from common import *

blueprint = Blueprint('catch_all', __name__)


@blueprint.route('/<path:path>')
async def catch_all(path):
    """
    Catch all other routes and return an error message.
    """
    
    return jsonify(ic({
        'message': f'<Error> `/{path}` endpoint does not exist in server replicas',
        'status': 'failure'
    })), 400
# END catch_all
