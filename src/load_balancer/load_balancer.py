import asyncio

import aiohttp
from aiodocker import Docker
from colorama import Fore, Style
from quart import Quart

from utils import *
from consts import *
from common import *

from endpoints import blueprint as all_blueprints

app = Quart(__name__)

# Register the blueprints
app.register_blueprint(all_blueprints)


@app.before_serving
async def my_startup():
    """
    Startup function to be run before the app starts.

    Start heartbeat background task.
    """

    # Register the heartbeat background task
    app.add_background_task(get_heartbeats)
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
# END my_shutdown


async def get_heartbeats():
    """
    Calls the heartbeat endpoint of all the replicas.
    If a replica does not respond, it is respawned.
    """

    global replicas
    global heartbeat_fail_count
    global serv_id

    if DEBUG:
        print(f'{Fore.CYAN}HEARTBEAT | '
              'Heartbeat background task started'
              f'{Style.RESET_ALL}',
              file=sys.stderr)

    await asyncio.sleep(0)

    try:
        while True:
            async with lock(Read):
                if DEBUG:
                    print(f'{Fore.CYAN}HEARTBEAT | '
                          f'Checking heartbeat every {HEARTBEAT_INTERVAL} seconds'
                          f'{Style.RESET_ALL}',
                          file=sys.stderr)

                # Get the list of server replica hostnames
                hostnames = replicas.getServerList().copy()
            # END async with lock(Read)

            # Generate heartbeat urls
            heartbeat_urls = [f'http://{server_name}:5000/heartbeat'
                              for server_name in hostnames]

            # Convert to aiohttp request
            timeout = aiohttp.ClientTimeout(connect=REQUEST_TIMEOUT)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                heartbeats = await gather_with_concurrency(
                    session, REQUEST_BATCH_SIZE, *heartbeat_urls)
            # END async with session

            # To allow other tasks to run
            await asyncio.sleep(0)

            semaphore = asyncio.Semaphore(DOCKER_TASK_BATCH_SIZE)

            async def wrapper(
                serv_id: int,
                server_name: str
            ):
                # To allow other tasks to run
                await asyncio.sleep(0)

                async with semaphore:
                    try:
                        await handle_flatline(serv_id, server_name)
                    except Exception as e:
                        if DEBUG:
                            print(f'{Fore.RED}ERROR | '
                                  f'{e}'
                                  f'{Style.RESET_ALL}',
                                  file=sys.stderr)
                    # END try-except
                # END async with semaphore
            # END wrapper

            flatlines = []

            async with lock(Write):
                for i, response in enumerate(heartbeats):
                    if response is None or not response.status == 200:
                        # Increment the fail count for the server replica
                        heartbeat_fail_count[hostnames[i]] = \
                            heartbeat_fail_count.get(hostnames[i], 0) + 1

                        # If fail count exceeds the max count, respawn the server replica
                        if heartbeat_fail_count[hostnames[i]] >= MAX_FAIL_COUNT:
                            serv_id += 1
                            flatlines.append(wrapper(serv_id, hostnames[i]))
                    else:
                        # Reset the fail count for the server replica
                        heartbeat_fail_count[hostnames[i]] = 0
                    # END if-else
                # END for

                ic(heartbeat_fail_count)

                # Don't gather with lock held for optimization
                if len(flatlines) > 0:
                    await asyncio.gather(*flatlines, return_exceptions=True)
            # END async with lock(Write)

            # check heartbeat every `HEARTBEAT_INTERVAL` seconds
            await asyncio.sleep(HEARTBEAT_INTERVAL)
        # END while

    except asyncio.CancelledError:
        if DEBUG:
            print(f'{Fore.CYAN}HEARTBEAT | '
                  'Heartbeat background task stopped'
                  f'{Style.RESET_ALL}',
                  file=sys.stderr)
    # END try-except
# END get_heartbeats


async def handle_flatline(
    serv_id: int,
    hostname: str
):
    """
    Handles the flatline of a server replica.
    """

    # Allow other tasks to run
    await asyncio.sleep(0)

    if DEBUG:
        print(f'{Fore.LIGHTRED_EX}FLATLINE | '
              f'Flatline of server replica `{hostname}` detected'
              f'{Style.RESET_ALL}',
              file=sys.stderr)

    try:
        async with Docker() as docker:
            container_config = {
                'image': 'server:v1',
                'detach': True,
                'env': [f'SERVER_ID={serv_id}',
                        'DEBUG=true'],
                'hostname': hostname,
                'tty': True,
            }

            # create the container
            container = await docker.containers.create_or_replace(
                name=hostname,
                config=container_config,
            )

            # Attach the container to the network and set the alias
            my_net = await docker.networks.get('my_net')

            await my_net.connect({
                'Container': container.id,
                'EndpointConfig': {
                    'Aliases': [hostname]
                }
            })

            if DEBUG:
                print(f'{Fore.LIGHTGREEN_EX}RECREATE | '
                      f'Created container for {hostname}'
                      f'{Style.RESET_ALL}',
                      file=sys.stderr)

            # start the container
            await container.start()

            if DEBUG:
                print(f'{Fore.MAGENTA}RESPAWN | '
                      f'Started container for {hostname}'
                      f'{Style.RESET_ALL}',
                      file=sys.stderr)
        # END async with docker

    except Exception as e:
        if DEBUG:
            print(f'{Fore.RED}ERROR | '
                  f'{e}'
                  f'{Style.RESET_ALL}',
                  file=sys.stderr)
    # END try-except
# END handle_flatline


if __name__ == '__main__':
    # Take port number from argument if provided
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5000

    # Run the server
    app.run(host='0.0.0.0', port=port,
            use_reloader=False, debug=DEBUG)
