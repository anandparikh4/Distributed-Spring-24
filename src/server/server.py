import os
from flask import Flask, Response, jsonify

app = Flask(__name__)

os.environ['SERVER_ID'] = '1234'


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
    app.run(port=8080, debug=True)
