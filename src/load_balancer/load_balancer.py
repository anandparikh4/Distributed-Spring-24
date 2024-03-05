import asyncio

import asyncpg
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

    # Register the blueprints
    app.register_blueprint(all_blueprints)

    # Register the heartbeat background task
    app.add_background_task(get_heartbeats)

    # Connect to the database
    app.pool = await asyncpg.create_pool(  # type: ignore
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        host=DB_HOST,
        port=DB_PORT
    )

    if app.pool is None:  # type: ignore
        print(f'{Fore.RED}ERROR | '
              f'Failed to connect to the database'
              f'{Style.RESET_ALL}',
              file=sys.stderr)
        sys.exit(1)
    # END if

# END my_startup


@app.after_serving
async def my_shutdown():
    """
    Shutdown function to be run after the app stops.

    1. Stop the heartbeat background task.
    2. Stop all server replicas.
    """

    # Stop the heartbeat background task
    app.background_tasks.pop().cancel()

    # Stop all server replicas
    semaphore = asyncio.Semaphore(DOCKER_TASK_BATCH_SIZE)

    async def wrapper(
        docker: Docker,
        server_name: str
    ):
        # Allow other tasks to run
        await asyncio.sleep(0)

        async with semaphore:
            try:
                container = await docker.containers.get(server_name)

                await container.stop(timeout=STOP_TIMEOUT)
                await container.delete(force=True)

                if DEBUG:
                    print(f'{Fore.LIGHTYELLOW_EX}REMOVE | '
                          f'Deleted container for {server_name}'
                          f'{Style.RESET_ALL}',
                          file=sys.stderr)
            except Exception as e:
                if DEBUG:
                    print(f'{Fore.RED}ERROR | '
                          f'{e}'
                          f'{Style.RESET_ALL}',
                          file=sys.stderr)
            # END try-except
        # END async with semaphore
    # END wrapper

    async with Docker() as docker:
        tasks = [wrapper(docker, server_name)
                 for server_name in replicas.getServerList()]
        await asyncio.gather(*tasks, return_exceptions=True)
    # END async with Docker

    # close the pool
    await app.pool.close()  # type: ignore
# END my_shutdown


if __name__ == '__main__':
    # Take port number from argument if provided
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5000

    # Run the server
    app.run(host='0.0.0.0', port=port,
            use_reloader=False, debug=DEBUG)
