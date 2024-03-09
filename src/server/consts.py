import os

# Server constants
SERVER_ID = os.environ.get('SERVER_ID', '0')
HOSTNAME = os.environ.get('HOSTNAME', 'localhost')

# If DEBUG mode is enabled
DEBUG = os.environ.get('DEBUG', 'false').lower() == 'true'

# Database constants
DB_HOST = os.environ.get('POSTGRES_HOST', 'localhost')
DB_PORT = int(os.environ.get('POSTGRES_PORT', 5432))
DB_USER = os.environ.get('POSTGRES_USER', 'postgres')
DB_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'postgres')
DB_NAME = os.environ.get('POSTGRES_DB_NAME', 'postgres')