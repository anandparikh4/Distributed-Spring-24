from quart import Quart

from endpoints import blueprint as all_blueprints
from utils import *

app = Quart(__name__)


@app.before_serving
async def my_startup():
    """
    Startup function to be run before the app starts.

    Start heartbeat background task.
    """

    try:
        # Register the blueprints
        app.register_blueprint(all_blueprints)

        common.pool = asyncpg.create_pool(
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            host=DB_HOST,
            port=DB_PORT,
        )

        await common.pool

        if DEBUG:
            print(f'{Fore.LIGHTYELLOW_EX}CONNECT | '
                  f'Connected to the database'
                  f'{Style.RESET_ALL}',
                  file=sys.stderr)

    except Exception as e:
        print(f'{Fore.RED}ERROR | '
              f'{e.__class__.__name__}: {e}'
              f'{Style.RESET_ALL}',
              file=sys.stderr)

        print(f'{Fore.RED}ERROR | '
              f'Failed to start the load balancer. Exiting...'
              f'{Style.RESET_ALL}',
              file=sys.stderr)

        # Exit the program
        sys.exit(1)
    # END try-except
# END my_startup


@app.after_serving
async def my_shutdown():
    """
    Shutdown function to be run after the app stops.

    1. Stop the heartbeat background task.
    2. Stop all server replicas.
    """

    # close the pool
    await common.pool.close()
# END my_shutdown


if __name__ == '__main__':
    # Take port number from argument if provided
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5000

    # Run the server
    app.run(host='0.0.0.0', port=port,
            use_reloader=False, debug=DEBUG)
