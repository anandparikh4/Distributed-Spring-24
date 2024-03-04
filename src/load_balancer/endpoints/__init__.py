from quart import Blueprint

from .config import blueprint as config_blueprint
from .database import blueprint as database_blueprint
from .other import blueprint as other_blueprint

blueprint = Blueprint('endpoints', __name__)

# Register the blueprints
blueprint.register_blueprint(config_blueprint)
blueprint.register_blueprint(database_blueprint)
blueprint.register_blueprint(other_blueprint)
