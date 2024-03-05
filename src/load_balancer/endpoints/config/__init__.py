from quart import Blueprint

from .add import blueprint as add_blueprint
from .rep import blueprint as rep_blueprint
from .rm import blueprint as rm_blueprint

blueprint = Blueprint('config', __name__)

# Register the blueprints
blueprint.register_blueprint(add_blueprint)
blueprint.register_blueprint(rep_blueprint)
blueprint.register_blueprint(rm_blueprint)
