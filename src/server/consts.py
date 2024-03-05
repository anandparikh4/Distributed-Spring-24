import os

SERVER_ID = os.environ.get('SERVER_ID', '0')
HOSTNAME = os.environ.get('HOSTNAME', 'localhost')
DEBUG = os.environ.get('DEBUG', 'false').lower() == 'true'