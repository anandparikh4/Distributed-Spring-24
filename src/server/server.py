import os
import sys

from icecream import ic
from quart import Quart, Response, jsonify

from consts import *
from common import *

app = Quart(__name__)



if __name__ == '__main__':
    # Take port number from argument if provided
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5000

    # Run the server
    app.run(host='0.0.0.0', port=port,
            use_reloader=False, debug=DEBUG)
