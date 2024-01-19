import os
import sys
from quart import Quart, Response, jsonify

app = Quart(__name__)

# os.environ['SERVER_ID'] = '1234'


@app.route('/home', methods=['GET'])
async def home():
    """
    Greet a client with its server ID.

    Response payload:
        message: Hello from Server: [ID]
        status: status of the response
    """

    return jsonify({
        'message': f"Hello from Server: {os.environ['SERVER_ID']}",
        'status': "successful"
    }), 200


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
    app.run(host='0.0.0.0', port=port, debug=True, use_reloader=False)
