import sys

from quart import Quart
from colorama import Fore, Style

from consts import *
from common import *

from endpoints import blueprint as endpoints_blueprint

app = Quart(__name__)

@app.before_serving
async def my_startup():
    '''
        Run startup tasks
    '''
    
    global pool

    # Register blueprints
    app.register_blueprint(endpoints_blueprint)

    # Connect to the database
    pool = asyncpg.create_pool(
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        host=DB_HOST,
        port=DB_PORT
    )

    await pool



@app.after_serving
async def my_shutdown():
    '''
        Run shutdown tasks
    '''

    # Close the database connection
    await pool.close()



if __name__ == '__main__':
    # Take port number from argument if provided
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5000

    # Run the server
    app.run(host='0.0.0.0', port=port,
            use_reloader=False, debug=DEBUG)
