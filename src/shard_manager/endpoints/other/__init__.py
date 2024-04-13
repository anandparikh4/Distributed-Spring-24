from quart import Blueprint

from .get_primary import blueprint as get_primary_blueprint
from .get_server import blueprint as get_server_blueprint

blueprint = Blueprint('other', __name__)

# Register the blueprints
blueprint.register_blueprint(get_primary_blueprint)
blueprint.register_blueprint(get_server_blueprint)
