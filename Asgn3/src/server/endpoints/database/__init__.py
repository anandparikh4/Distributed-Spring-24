from quart import Blueprint

from .copy import blueprint as copy_blueprint
from .delete import blueprint as del_blueprint
from .read import blueprint as read_blueprint
from .recover import blueprint as recover_blueprint
from .update import blueprint as update_blueprint
from .write import blueprint as write_blueprint

blueprint = Blueprint('database', __name__)

# Register blueprints
blueprint.register_blueprint(copy_blueprint)
blueprint.register_blueprint(del_blueprint)
blueprint.register_blueprint(read_blueprint)
blueprint.register_blueprint(recover_blueprint)
blueprint.register_blueprint(update_blueprint)
blueprint.register_blueprint(write_blueprint)
