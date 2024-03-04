from quart import Blueprint

from common import *

from .catch_all import blueprint as catch_all_blueprint
from .home import blueprint as home_blueprint

blueprint = Blueprint('other', __name__)

# Register the blueprints
blueprint.register_blueprint(catch_all_blueprint)
blueprint.register_blueprint(home_blueprint)
