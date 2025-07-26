from quart import Blueprint

from .read import blueprint as read_blueprint
from .write import blueprint as write_blueprint
from .update import blueprint as update_blueprint
from .delete import blueprint as delete_blueprint

blueprint = Blueprint('database', __name__)

# Register the blueprints
blueprint.register_blueprint(read_blueprint)
blueprint.register_blueprint(write_blueprint)
blueprint.register_blueprint(update_blueprint)
blueprint.register_blueprint(delete_blueprint)
