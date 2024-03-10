from quart import Blueprint, current_app, jsonify, request
from colorama import Fore, Style
import sys

from consts import *
from common import *

blueprint = Blueprint('write', __name__)

@blueprint.route('/write', methods=['POST'])
async def data_write():
    """
        Returns requested data entries from the server container

        Request payload:
            "shard_id" : "sh1"
            "curr_idx" : curr_idx
            "data" : [{"stud_id": low, ...},
                      {"stud_id": low+1, ...},
                      ...
                      {"stud_id": high, ...}]

        Response payload:
            
    """
    pass

