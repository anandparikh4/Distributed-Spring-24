from quart import Blueprint
from .config import blueprint as config_blueprint

blueprint = Blueprint('config', __name__)

blueprint.register_blueprint(config_blueprint)