import asyncio
import random
import sys
import grequests
from fifolock import FifoLock
from utils import Read, Write, random_hostname, err_payload
from quart import Quart, request, jsonify
from icecream import ic
from hash import ConsistentHashMap

app = Quart(__name__)
lock = FifoLock()

# ic.disable()

# List to store web server replica hostnames
replicas = ConsistentHashMap()


# Map to store heartbeat fail counts for each server replica.
heartbeat_fail_count: dict[str, int] = {}
MAX_COUNT = 5


@app.route('/rep', methods=['GET'])
async def rep():
    """
    Return the number and list of replica hostnames.

    Response payload:
        message:
            N: number of replicas
            replicas: list of replica hostnames
        status: status of the request
    """

    global replicas
    global heartbeat_fail_count

    async with lock(Read):
        # Return the response payload
        return jsonify({
            'message': {
                'N': len(replicas),
                'replicas': replicas.getServerList(),
            },
            'status': 'successful',
        }), 200
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

            ic("To add:", hostnames)

            # Add the hostnames to the list
            for hostname in hostnames:
                replicas.add(hostname)

                # Edit the flatline map
                heartbeat_fail_count[hostname] = 0

                # TODO: spawn new docker containers for the new hostnames
            # END for
        # END async with lock

        ic(replicas.getServerList())

        # Return the response payload
        return jsonify({
            'message': {
                'N': len(replicas),
                'replicas': replicas.getServerList()
            },
            'status': 'success'
        }), 200

    except Exception as e:
        return jsonify(err_payload(e)), 400
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

            ic("To delete:", hostnames)

            # Delete the hostnames from the list
            for hostname in hostnames:
                replicas.remove(hostname)

                # Edit the flatline map
                heartbeat_fail_count.pop(hostname, None)

                # TODO: kill docker containers for the deleted hostnames
        # END async with lock

        ic(replicas.getServerList())

        # Return the response payload
        return jsonify({
            'message': {
                'N': len(replicas),
                'replicas': replicas.getServerList()
            },
            'status': 'success'
        }), 200

    except Exception as e:
        return jsonify(err_payload(e)), 400
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

        server_name = replicas.find(request_id)

        if server_name is None:
            raise Exception('No servers are available')

        # Send the request to the server asynchronously
        # serv_response = requests.get(f'http://{server_name}:8080/home')
        serv_request = grequests.get('http://127.0.0.1:8080/home')
        # async stuff happens here (I think lol)
        serv_response = grequests.map([serv_request])[0]

        # To allow other tasks to run
        await asyncio.sleep(0)

        if serv_response is None:
            raise Exception('Server did not respond')

        return jsonify(serv_response.json()), 200

    except Exception as e:
        return jsonify(err_payload(e)), 400
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

            port = 12345

            heartbeats = [grequests.get(f'http://{server_name}:8080/heartbeat')
                          for server_name in replicas.getServerList()]
            # heartbeats = [grequests.get(f'http://127.0.0.1:{port + i}/heartbeat')
            #               for i in range(len(hostnames))]  # TODO: change to server_name

            for i, response in grequests.imap_enumerated(heartbeats):
                if response is None:
                    # Increment the fail count for the server replica
                    heartbeat_fail_count[hostnames[i]] = \
                        heartbeat_fail_count.get(hostnames[i], 0) + 1

                    # If fail count exceeds the max count, respawn the server replica
                    if heartbeat_fail_count[hostnames[i]] >= MAX_COUNT:
                        handle_flatline(hostnames[i])
                else:
                    # Reset the fail count for the server replica
                    heartbeat_fail_count[hostnames[i]] = 0
                # END if-else

                # To allow other tasks to run
                await asyncio.sleep(0)
            # END for

            ic(heartbeat_fail_count)
        # END async with lock

        await asyncio.sleep(5)
# END get_heartbeats


def handle_flatline(server_name: str):
    """
    Handles the flatline of a server replica.
    """

    msg = f'Flatline of server replica `{server_name}` detected'
    ic(msg)

    # TODO: respawn the server replica using docker

# END handle_flatline


if __name__ == '__main__':
    # Take port number from argument if provided
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5000

    # Run the server
    app.run(port=port, debug=True, use_reloader=False)
