import random
from fifolock import FifoLock
from flask import Flask, request, jsonify
from utils import Read, Write, random_hostname
from icecream import ic
from hash import ConsistentHashMap

app = Flask(__name__)
app.debug = True
lock = FifoLock()


# List to store web server replica hostnames
# TODO: Replace with consistent hash data structure
# replicas: list[str] = []
replicas= ConsistentHashMap()


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
    If `n > remaining slots`:
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

    # Get the request payload
    payload: dict = request.get_json()
    ic(payload)

    # Get the number of servers to add
    n = int(payload.get('n', -1))

    # Get the list of server replica hostnames to add
    hostnames: list[str] = list(payload.get('hostnames', []))

    if n <= 0:
        return jsonify({
            'message': '<Error> Number of servers to add must be greater than 0',
            'status': 'failure'
        }), 400

    if len(hostnames) > n:
        return jsonify({
            'message': '<Error> Length of hostname list is more than new instances to add',
            'status': 'failure'
        }), 400

    if n > replicas.remaining():
        return jsonify({
            'message': f'<Error> Insufficient slots. Only {replicas.remaining()} slots left',
            'status': 'failure'
        }), 400

    # Generate `n - len(hostnames)` random hostnames
    new_hostnames = set()
    while len(new_hostnames) < n - len(hostnames):
        new_hostnames.add(random_hostname())

    async with lock(Write):
        # Add `new_hostnames` random hostnames to the list.
        hostnames.extend(new_hostnames)

        ic("To add:", hostnames)

        # Add the hostnames to the list
        for hostname in hostnames:
            # replicas.append(hostname)
            replicas.add(hostname)
            # TODO: spawn new docker containers for the new hostnames
        # END for
    # END async with lock

    # ic(replicas)

    # Return the response payload
    return jsonify({
        'message': {
            'N': len(replicas),
            'replicas': replicas.getServerList()
        },
        'status': 'success'
    }), 200
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

    # Get the request payload
    payload: dict = request.get_json()
    ic(payload)

    # Get the number of servers to delete
    n = int(payload.get('n', -1))

    # Get the list of server replica hostnames to delete
    hostnames: list[str] = list(payload.get('hostnames', []))

    if n <= 0:
        return jsonify({
            'message': '<Error> Number of servers to delete must be greater than 0',
            'status': 'failure'
        }), 400

    if n > len(replicas):
        return jsonify({
            'message': '<Error> Number of servers to delete must be less than or equal to number of replicas',
            'status': 'failure'
        }), 400

    if len(hostnames) > n:
        return jsonify({
            'message': '<Error> Length of hostname list is more than instances to delete',
            'status': 'failure'
        }), 400

    # return first hostname that is not in replicas
    for hostname in hostnames:
        if hostname not in replicas.getServerList():
            return jsonify({
                'message': f'<Error> Hostname `{hostname}` is not in replicas',
                'status': 'failure'
            }), 400
    # END for

    # remove `hostnames` from `replicas`
    choices = replicas.getServerList().copy()
    for hostname in hostnames:
        choices.remove(hostname)
    # END for

    async with lock(Write):
        # Choose `n - len(hostnames)` random hostnames from the list without replacement
        random_hostnames = random.sample(choices, k=n - len(hostnames))

        # Add the random hostnames to the list of hostnames to delete
        hostnames.extend(random_hostnames)

        ic("To delete:", hostnames)

        # Delete the hostnames from the list
        for hostname in hostnames:
            replicas.remove(hostname)
            # TODO: kill docker containers for the deleted hostnames
    # END async with lock

    # ic(replicas)

    # Return the response payload
    return jsonify({
        'message': {
            'N': len(replicas),
            'replicas': replicas.getServerList()
        },
        'status': 'success'
    }), 200
# END delete


@app.route('/{path}', methods=['GET', 'POST', 'DELETE'])
async def catch_all(path: str):
    """
    Catch all requests to the load balancer.
    """
    return jsonify({
        'message': f'<Error> Invalid endpoint: {path}',
        'status': 'failure'
    }), 400
# END catch_all


if __name__ == '__main__':
    app.run(port=8080)
