from quart import Blueprint

from .heartbeat import blueprint as heartbeat_blueprint
from .home import blueprint as home_blueprint

blueprint = Blueprint('others', __name__)

# Register blueprints
blueprint.register_blueprint(heartbeat_blueprint)
blueprint.register_blueprint(home_blueprint)