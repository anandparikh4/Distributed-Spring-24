import aiohttp
import asyncio
import os
import random
import sys
from aiodocker import Docker
from fifolock import FifoLock
from icecream import ic
from quart import Quart, request, jsonify
from colorama import Fore, Style

from hash import ConsistentHashMap, requestHashList, serverHashList
from utils import *

app = Quart(__name__)
lock = FifoLock()

DEBUG = os.environ.get('DEBUG', 'false').lower() == 'true'
HASH_NUM = int(os.environ.get('HASH_NUM', 0))

ic.configureOutput(prefix='[LB] | ')

# Disable icecream debug messages if DEBUG is not set to true
if not DEBUG:
    ic.disable()

# List to store web server replica hostnames
replicas = ConsistentHashMap(
    request_hash=requestHashList[HASH_NUM],
    server_hash=serverHashList[HASH_NUM])

# Map to store heartbeat fail counts for each server replica.
heartbeat_fail_count: dict[str, int] = {}

# server unique id generator
serv_id = 3  # already have 3 servers running

# max number of consecutive heartbeat fails
MAX_FAIL_COUNT = 5

# interval between heartbeat checks in seconds
HEARTBEAT_INTERVAL = 10

# timeout for stopping a container in seconds
STOP_TIMEOUT = 5

# timeout for requests in seconds
REQUEST_TIMEOUT = 1

# number of requests to send in a batch
REQUEST_BATCH_SIZE = 10

# number of docker tasks to perform in a batch
DOCKER_TASK_BATCH_SIZE = 10


@app.route('/rep', methods=['GET'])
async def rep():
    """
    Return the number and list of replica hostnames.

    `Response payload:`
        `message:`
            `N: number of replicas`
            `replicas: list of replica hostnames`
        `status: status of the request`
    """

    global replicas

    async with lock(Read):
        # Return the response payload
        return jsonify(ic({
            'message': {
                'N': len(replicas),
                'replicas': replicas.getServerList(),
            },
            'status': 'successful',
        })), 200
    # END async with lock
# END rep


@app.route('/add', methods=['POST'])
async def add():
    """
    Add new server replica hostname(s) to the list.

    If `len(hostnames) <= n`:
        Add `hostnames` and `n - len(hostnames)` random hostnames to the list.
    If `n <= 0`:
        Return an error message.
    If `len(hostnames) > n`:
        Return an error message.
    If `hostnames` contains duplicates:
        Return an error message.
    If `n > remaining slots`:
        Return an error message.
    If `hostname` in `hostnames` already exists in `replicas`:
        Do not add any replicas to the list.
        Return an error message.

    Random hostnames are generated using the `random_hostname()` function.

    `Request payload:`
        `n: number of servers to add`
        `hostnames: list of server replica hostnames to add (<= n) [optional]`

    `Response payload:`
        `message:`
            `N: number of replicas`
            `replicas: list of replica hostnames`
        `status: status of the request`

    `Error payload:`
        `message: error message`
        `status: status of the request`
    """

    global replicas
    global heartbeat_fail_count
    global serv_id

    # Allow other tasks to run
    await asyncio.sleep(0)

    try:
        # Get the request payload
        payload: dict = await request.get_json()
        ic(payload)

        if payload is None:
            raise Exception('Payload is empty')

        # Get the number of servers to add
        n = int(payload.get('n', -1))

        # Get the list of server replica hostnames to add
        hostnames: list[str] = list(payload.get('hostnames', []))

        if n <= 0:
            raise Exception(
                'Number of servers to add must be greater than 0')

        if len(hostnames) > n:
            raise Exception(
                'Length of hostname list is more than instances to add')

        if len(hostnames) != len(set(hostnames)):
            raise Exception('Hostname list contains duplicates')

        # Generate `n - len(hostnames)` random hostnames
        new_hostnames: set[str] = set()
        while len(new_hostnames) < n - len(hostnames):
            new_hostnames.add(random_hostname())

        # Add `new_hostnames` to the list.
        hostnames.extend(new_hostnames)

        async with lock(Write):
            # Check is slots are available
            if n > replicas.remaining():
                raise Exception(
                    f'Insufficient slots. Only {replicas.remaining()} slots left')

            hostnames_set = set(hostnames)
            replicas_set = set(replicas.getServerList())

            # Check if all `hostnames` are in `replicas`
            if not hostnames_set.isdisjoint(replicas_set):
                raise Exception(
                    f'Hostnames `{hostnames_set & replicas_set}` are already in replicas')

            ic("To add: ", hostnames)

            semaphore = asyncio.Semaphore(DOCKER_TASK_BATCH_SIZE)

            async def spawn_container(
                docker: Docker,
                serv_id: int,
                hostname: str
            ):
                # Allow other tasks to run
                await asyncio.sleep(0)

                async with semaphore:
                    try:
                        # spawn new docker containers for the new hostnames
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
                            print(f'{Fore.LIGHTGREEN_EX}CREATE | '
                                  f'Created container for {hostname}'
                                  f'{Style.RESET_ALL}',
                                  file=sys.stderr)

                        # start the container
                        await container.start()

                        if DEBUG:
                            print(f'{Fore.MAGENTA}SPAWN | '
                                  f'Started container for {hostname}'
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
            # END spawn_container

            async with Docker() as docker:
                # Define tasks
                tasks = []

                # Add the hostnames to the list
                for hostname in hostnames:
                    replicas.add(hostname)

                    # Edit the flatline map
                    heartbeat_fail_count[hostname] = 0

                    # increment server id
                    serv_id += 1

                    tasks.append(spawn_container(docker, serv_id, hostname))
                # END for

                # Wait for all tasks to complete
                await asyncio.gather(*tasks, return_exceptions=True)
            # END async with Docker

            final_hostnames = ic(replicas.getServerList())
        # END async with lock(Write)

        # Return the response payload
        return jsonify(ic({
            'message': {
                'N': len(replicas),
                'replicas': final_hostnames
            },
            'status': 'success'
        })), 200

    except Exception as e:
        if DEBUG:
            print(f'{Fore.RED}ERROR | '
                  f'{e}'
                  f'{Style.RESET_ALL}',
                  file=sys.stderr)

        return jsonify(ic(err_payload(e))), 400
    # END try-except
# END add


@app.route('/rm', methods=['DELETE'])
async def delete():
    """
    Delete server replica hostname(s) from the list.

    If `len(hostnames) <= n`:
        Delete `hostnames` and `n - len(hostnames)` random hostnames from the list.
    If `n <= 0`:
        Return an error message.
    If `n > len(replicas)`:
        Return an error message.
    If `len(hostnames) > n`:
        Return an error message.
    If for any hostname in `hostnames`, `hostname not in replicas`:
        Do not delete any replicas from the list.
        Return an error message.

    Random hostnames are deleted from the list of replicas.

    `Request payload:`
        `n: number of servers to delete`
        `hostnames: list of server replica hostnames to delete (<= n) [optional]`

    `Response payload:`
        `message:`
            `N: number of replicas`
            `replicas: list of replica hostnames`
        `status: status of the request`

    `Error payload:`
        `message: error message`
        `status: status of the request`
    """

    global replicas
    global heartbeat_fail_count

    # Allow other tasks to run
    await asyncio.sleep(0)

    try:
        # Get the request payload
        payload: dict = await request.get_json()
        ic(payload)

        if payload is None:
            raise Exception('Payload is empty')

        # Get the number of servers to delete
        n = int(payload.get('n', -1))

        # Get the list of server replica hostnames to delete
        hostnames: list[str] = list(payload.get('hostnames', []))

        if n <= 0:
            raise Exception(
                'Number of servers to delete must be greater than 0')

        if n > len(replicas):
            raise Exception(
                'Number of servers to delete must be less than or equal to number of replicas')

        if len(hostnames) > n:
            raise Exception(
                'Length of hostname list is more than instances to delete')

        async with lock(Write):
            choices = set(replicas.getServerList())

            # Convert hostnames to set for faster lookup
            hostnames_set = set(hostnames)

            # Check if all `hostnames` are in `replicas`
            if not hostnames_set.issubset(choices):
                raise Exception(
                    f'Hostnames `{hostnames_set - choices}` are not in replicas')

            # remove `hostnames` from `choices`
            choices = list(choices - hostnames_set)

            # Choose `n - len(hostnames)` random hostnames from the list without replacement
            random_hostnames = random.sample(choices, k=n - len(hostnames))

            # Add the random hostnames to the list of hostnames to delete
            hostnames.extend(random_hostnames)

            ic("To delete: ", hostnames)

            semaphore = asyncio.Semaphore(DOCKER_TASK_BATCH_SIZE)

            async def remove_container(
                docker: Docker,
                hostname: str
            ):
                # Allow other tasks to run
                await asyncio.sleep(0)

                async with semaphore:
                    try:
                        # stop docker containers for the deleted hostnames
                        container = await docker.containers.get(hostname)

                        await container.stop(timeout=STOP_TIMEOUT)
                        await container.delete(force=True)

                        if DEBUG:
                            print(f'{Fore.LIGHTYELLOW_EX}REMOVE | '
                                  f'Deleted container for {hostname}'
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
            # END stop_container

            async with Docker() as docker:
                # Define tasks
                tasks = []

                # Delete the hostnames from the list
                for hostname in hostnames:
                    replicas.remove(hostname)

                    # Edit the flatline map
                    heartbeat_fail_count.pop(hostname, None)

                    tasks.append(remove_container(docker, hostname))
                # END for

                # Wait for all tasks to complete
                await asyncio.gather(*tasks, return_exceptions=True)
            # END async with Docker

            final_hostnames = ic(replicas.getServerList())
        # END async with lock(Write)

        # Return the response payload
        return jsonify(ic({
            'message': {
                'N': len(replicas),
                'replicas': final_hostnames
            },
            'status': 'success'
        })), 200

        # END async with lock

    except Exception as e:
        if DEBUG:
            print(f'{Fore.RED}ERROR | '
                  f'{e}'
                  f'{Style.RESET_ALL}',
                  file=sys.stderr)

        return jsonify(ic(err_payload(e))), 400
    # END try-except
# END delete


@app.route('/home', methods=['GET'])
async def home():
    """
    Load balance the request to the server replicas.

    `Response payload:`
        `message: message from server`
        `status: status of the request`

    `Error payload:`
        `message: error message`
        `status: status of the request`
    """

    global replicas

    # To allow other tasks to run
    await asyncio.sleep(0)

    try:
        # Generate a random request id
        request_id = random.randint(100000, 999999)
        ic(request_id)

        async with lock(Read):
            server_name = replicas.find(request_id)

        if server_name is None:
            raise Exception('No servers are available')

        ic(server_name)

        async def wrapper(
            session: aiohttp.ClientSession,
            server_name: str
        ):
            # To allow other tasks to run
            await asyncio.sleep(0)

            async with session.get(f'http://{server_name}:5000/home') as response:
                await response.read()

            return response
        # END wrapper

        # Convert to aiohttp request
        timeout = aiohttp.ClientTimeout(connect=REQUEST_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            tasks = [asyncio.create_task(wrapper(session, server_name))]
            serv_response = await asyncio.gather(*tasks, return_exceptions=True)
            serv_response = serv_response[0] if not isinstance(
                serv_response[0], BaseException) else None
        # END async with

        if serv_response is None:
            raise Exception('Server did not respond')

        return jsonify(ic(await serv_response.json())), 200

    except Exception as e:
        if DEBUG:
            print(f'{Fore.RED}ERROR | '
                  f'{e}'
                  f'{Style.RESET_ALL}',
                  file=sys.stderr)

        return jsonify(ic(err_payload(e))), 400
    # END try-except
# END home


@app.route('/<path:path>')
async def catch_all(path):
    """
    Catch all other routes and return an error message.
    """

    return jsonify(ic({
        'message': f'<Error> `/{path}` endpoint does not exist in server replicas',
        'status': 'failure'
    })), 400
# END catch_all


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
