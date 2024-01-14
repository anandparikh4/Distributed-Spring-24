import os
from flask import Flask, Response, jsonify

app = Flask(__name__)

@app.route('/home', methods=['GET'])
def home():
    return jsonify({
        'message': f"Hello from Server: {os.environ['SERVER_ID']}",
        'status': "successful"
    }), 200

@app.route('/heartbeat', methods=['GET'])
def heartbeat():
    return Response(status=200)
