import aiodocker
import aiohttp
import asyncio
import os
import random
import sys
from fifolock import FifoLock
from icecream import ic
from quart import Quart, request, jsonify

from hash import ConsistentHashMap
from utils import *

app = Quart(__name__)
lock = FifoLock()

ic.configureOutput(prefix='[LB] | ',)
# Disable icecream debug messages if DEBUG is not set to true
if not os.environ.get('DEBUG', 'false').lower() == 'true':
    ic.disable()

# List to store web server replica hostnames
replicas = ConsistentHashMap()

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

# async stuff is crazy so we need this
loop = asyncio.get_event_loop()


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
    If `hostname` already exists in `replicas`:
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

    try:
        # Get the request payload
        payload: dict = await request.get_json()
        ic(payload)

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

            # Get Docker client
            docker = aiodocker.Docker()

            # Add the hostnames to the list
            for hostname in hostnames:
                replicas.add(hostname)

                # Edit the flatline map
                heartbeat_fail_count[hostname] = 0

                # increment server id
                serv_id += 1

                # TODO: spawn new docker containers for the new hostnames

                container_config = {
                    'image': 'server:v1',
                    'detach': True,
                    'env': [f'SERVER_ID={serv_id}',
                            'DEBUG=true'],
                    'hostname': hostname,
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

                print(f'CREATE | Created container for {hostname}',
                      file=sys.stderr)

                # start the container
                await container.start()

                # TODO: do error handling for container start

                print(f'SPAWN | Started container for {hostname}',
                      file=sys.stderr)

                await asyncio.sleep(0)

            # END for

            # close docker session
            await docker.close()

            # this also should be locked
            ic(replicas.getServerList())

            # Return the response payload
            return jsonify(ic({
                'message': {
                    'N': len(replicas),
                    'replicas': replicas.getServerList()
                },
                'status': 'success'
            })), 200

        # END async with lock

    except Exception as e:
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

    try:
        # Get the request payload
        payload: dict = await request.get_json()
        ic(payload)

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

            # Get Docker client
            docker = aiodocker.Docker()

            # Delete the hostnames from the list
            for hostname in hostnames:
                replicas.remove(hostname)

                # Edit the flatline map
                heartbeat_fail_count.pop(hostname, None)

                # TODO: kill docker containers for the deleted hostnames
                container = await docker.containers.get(hostname)

                await container.stop(timeout=STOP_TIMEOUT)
                await container.delete(force=True)

                # TODO: do error handling for container stop and delete

                print(f'REMOVE | Deleted container for {hostname}',
                      file=sys.stderr)

                # await docker.close()

                await asyncio.sleep(0)

            # END for

            # close docker session
            await docker.close()

            # this also should be locked
            ic(replicas.getServerList())

            # Return the response payload
            return jsonify(ic({
                'message': {
                    'N': len(replicas),
                    'replicas': replicas.getServerList()
                },
                'status': 'success'
            })), 200

        # END async with lock

    except Exception as e:
        return jsonify(ic(err_payload(e))), 400
    # END try-except
# END delete


@app.route('/home', methods=['GET'])
async def home():
    """
    Load balance the request to the server replicas.

    `Request payload:`
        `request_id: id of the request`

    `Response payload:`
        `message: error message`
        `status: status of the request`

    `Error payload:`
        `message: error message`
        `status: status of the request`
    """

    global replicas
    global heartbeat_fail_count

    try:
        # Get the request payload
        payload: dict = await request.get_json()
        ic(payload)

        request_id = int(payload.get("request_id", -1))

        server_name = None
        async with lock(Read):
            server_name = replicas.find(request_id)

        if server_name is None:
            raise Exception('No servers are available')

        # Convert to aiohttp request
        timeout = aiohttp.ClientTimeout(connect=REQUEST_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            serv_response = await gather_with_concurrency(
                session, REQUEST_BATCH_SIZE, *[f'http://{server_name}:5000/home'])
            serv_response = serv_response[0]

        # To allow other tasks to run
        await asyncio.sleep(0)

        if serv_response is None:
            raise Exception('Server did not respond')

        return jsonify(ic(await serv_response.json())), 200

    except Exception as e:
        return jsonify(ic(err_payload(e))), 400
    # END try-except
# END home


@app.route('/<path:path>')
async def catch_all(path):
    """
    Catch all other routes and return an error message.
    """

    return jsonify({
        'message': f'<Error> `/{path}` endpoint does not exist in server replicas',
        'status': 'failure'
    }), 400
# END catch_all


@app.before_serving
async def my_startup():
    """
    Startup function to be run before the app starts.
    """

    # Register the heartbeat background task
    app.add_background_task(get_heartbeats)

    # To allow other tasks to run
    await asyncio.sleep(0)
# END my_startup


async def get_heartbeats():
    """
    Calls the heartbeat endpoint of all the replicas.
    If a replica does not respond, it is respawned.
    """

    global replicas
    global heartbeat_fail_count

    while True:
        async with lock(Read):
            # Get the list of server replica hostnames
            hostnames = replicas.getServerList().copy()

            heartbeat_urls = [f'http://{server_name}:5000/heartbeat'
                              for server_name in hostnames]
            heartbeats = [None] * len(hostnames)

            # Convert to aiohttp request
            timeout = aiohttp.ClientTimeout(connect=REQUEST_TIMEOUT)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                heartbeats = await gather_with_concurrency(
                    session, REQUEST_BATCH_SIZE, *heartbeat_urls)
            # END async with

            # To allow other tasks to run
            await asyncio.sleep(0)

            for i, response in enumerate(heartbeats):
                if response is None or not response.status == 200:
                    # Increment the fail count for the server replica
                    heartbeat_fail_count[hostnames[i]] = \
                        heartbeat_fail_count.get(hostnames[i], 0) + 1

                    # If fail count exceeds the max count, respawn the server replica
                    if heartbeat_fail_count[hostnames[i]] >= MAX_FAIL_COUNT:
                        await handle_flatline(hostnames[i])
                else:
                    # Reset the fail count for the server replica
                    heartbeat_fail_count[hostnames[i]] = 0
                # END if-else

            # END for

            ic(heartbeat_fail_count)
        # END async with lock

        await asyncio.sleep(HEARTBEAT_INTERVAL)
    # END while
# END get_heartbeats


async def handle_flatline(server_name: str):
    """
    Handles the flatline of a server replica.
    """

    print(
        f'FLATLINE | Flatline of server replica `{server_name}` detected', file=sys.stderr)

    # Get Docker client
    docker = aiodocker.Docker()

    # TODO: respawn the server replica using docker
    container = await docker.containers.get(server_name)

    await container.restart(timeout=STOP_TIMEOUT)

    # TODO: do error handling for container restart

    print(f'RESTART | Restarted container for {server_name}', file=sys.stderr)

    # close docker session
    await docker.close()

    await asyncio.sleep(0)

# END handle_flatline


if __name__ == '__main__':
    # Take port number from argument if provided
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5000

    # Run the server
    app.run(host='0.0.0.0', port=port, debug=True,
            use_reloader=False, loop=loop)
