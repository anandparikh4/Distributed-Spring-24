from quart import Blueprint

from .delete import blueprint as delete_blueprint
from .read import blueprint as read_blueprint
from .read_serv_id import blueprint as read_serv_id_blueprint
from .update import blueprint as update_blueprint
from .write import blueprint as write_blueprint

blueprint = Blueprint('database', __name__)

# Register the blueprints
blueprint.register_blueprint(delete_blueprint)
blueprint.register_blueprint(read_blueprint)
blueprint.register_blueprint(read_serv_id_blueprint)
blueprint.register_blueprint(update_blueprint)
blueprint.register_blueprint(write_blueprint)
