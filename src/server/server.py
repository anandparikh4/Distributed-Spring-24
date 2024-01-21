import os
import sys
from icecream import ic
from quart import Quart, Response, jsonify

app = Quart(__name__)

SERVER_ID = os.environ.get('SERVER_ID', '0')
DEBUG = os.environ.get('DEBUG', 'false').lower() == 'true'

ic.configureOutput(prefix=f'[Server: {SERVER_ID}] | ')

# Disable icecream debug messages if DEBUG is not set to true
if not DEBUG:
    ic.disable()


@app.route('/home', methods=['GET'])
async def home():
    """
    Greet a client with its server ID.

    Response payload:
        message: Hello from Server: [ID]
        status: status of the response
    """

    return jsonify(ic({
        'message': f"Hello from Server: {SERVER_ID}",
        'status': "successful"
    })), 200


@app.route('/heartbeat', methods=['GET'])
async def heartbeat():
    """
    Send heartbeat response upon request.

    Response payload:
        message: [EMPTY]
        status: status of the response
    """

    return Response(status=200)

if __name__ == '__main__':
    # Take port number from argument if provided
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5000

    # Run the server
    app.run(host='0.0.0.0', port=port,
            use_reloader=False, debug=DEBUG)
