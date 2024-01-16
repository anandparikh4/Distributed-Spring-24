import random
from fifolock import FifoLock
from flask import Flask, request, jsonify
from utils import Read, Write, random_hostname, err_payload
from icecream import ic
from hash import ConsistentHashMap

app = Flask(__name__)
lock = FifoLock()

# ic.disable()

# List to store web server replica hostnames
# TODO: Replace with consistent hash data structure
# replicas: list[str] = []
replicas = ConsistentHashMap()


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
    If hostnames contains duplicates:
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

    try:
        # Get the request payload
        payload: dict = request.get_json()
        ic(payload)

        # Get the number of servers to add
        n = int(payload.get('n', -1))

        # Get the list of server replica hostnames to add
        hostnames: list[str] = list(payload.get('hostnames', []))

        if n <= 0:
            raise Exception(
                '<Error> Number of servers to add must be greater than 0')

        if len(hostnames) > n:
            raise Exception(
                '<Error> Length of hostname list is more than instances to add')

        if len(hostnames) != len(set(hostnames)):
            raise Exception('<Error> Hostname list contains duplicates')

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
                    f'<Error> Insufficient slots. Only {replicas.remaining()} slots left')

            ic("To add:", hostnames)

            # Add the hostnames to the list
            for hostname in hostnames:
                # replicas.append(hostname)
                replicas.add(hostname)
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

    try:
        # Get the request payload
        payload: dict = request.get_json()
        ic(payload)

        # Get the number of servers to delete
        n = int(payload.get('n', -1))

        # Get the list of server replica hostnames to delete
        hostnames: list[str] = list(payload.get('hostnames', []))

        if n <= 0:
            raise Exception(
                '<Error> Number of servers to delete must be greater than 0')

        if n > len(replicas):
            raise Exception(
                '<Error> Number of servers to delete must be less than or equal to number of replicas')

        if len(hostnames) > n:
            raise Exception(
                '<Error> Length of hostname list is more than instances to delete')

        async with lock(Write):
            choices = replicas.getServerList().copy()

            # return first hostname that is not in replicas
            for hostname in hostnames:
                if hostname not in choices:
                    raise Exception(
                        f'<Error> Hostname `{hostname}` is not in replicas')
            # END for

            # remove `hostnames` from `replicas`
            for hostname in hostnames:
                choices.remove(hostname)
            # END for

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
# END delete


@app.route('/find', methods=['GET'])
async def catch_all():
    """
    Catch all requests to the load balancer.
    """

    try:
        payload: dict = request.get_json()
        ic(payload)
        request_id = int(payload.get("request_id", -1))

        server_name = replicas.find(request_id)

        if server_name is None:
            raise Exception('<Error> No servers are available')

        return jsonify({
            'message': f'Sending to server: {server_name}',
            'status': 'successful'
        }), 200

    except Exception as e:
        return jsonify(err_payload(e)), 400
    # END try-except
# END catch_all


if __name__ == '__main__':
    app.run(debug=True)
