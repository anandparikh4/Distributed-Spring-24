import sys
import signal
import random
import requests
# import grequests

# URL of the server
url = 'http://127.0.0.1:5000'
endpoints = ['add', 'rm', 'rep']


# clean exit on ctrl-c
def signal_handler(sig, frame):
    print('Exiting...')
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)


def add_request(
    n: int,
    hostnames: list[str],
):
    """
    Send a request to the add endpoint.
    """
    payload = {
        'n': n,
        'hostnames': hostnames,
    }

    response = requests.post(url + '/add', json=payload)
    # response = grequests.post(url + '/add', json=payload)
    # response = grequests.map([response])[0]

    return f'Response: {response.text}Code: {response.status_code}'
# END add_request


def delete_request(
    n: int,
    hostnames: list[str],
):
    """
    Send a request to the delete endpoint.
    """

    payload = {
        'n': n,
        'hostnames': hostnames,
    }

    response = requests.delete(url + '/rm', json=payload)

    return f'Response: {response.text}Code: {response.status_code}'
# END delete_request


def view_request():
    """
    Send a request to the view endpoint.
    """

    response = requests.get(url + '/rep')

    return f'Response: {response.text}Code: {response.status_code}'
# END view_request


def parse_command(cmd: str):
    parts = cmd.split()
    parts = [part.strip() for part in parts]

    if not parts:
        raise ValueError("No command provided")

    command = parts[0].upper()
    if command == 'ADD':
        parts.append("")
        if len(parts) < 2:
            raise ValueError("Not enough arguments for ADD command")
        number = int(parts[1])
        items = parts[2:]
        items.pop()

        return ('ADD', number, items)

    elif command == 'DEL':
        parts.append("")
        if len(parts) < 2:
            raise ValueError("Not enough arguments for DEL command")
        number = int(parts[1])
        items = parts[2:]
        items.pop()

        return ('DEL', number, items)

    elif command == 'REP':
        if len(parts) > 1:
            raise ValueError("REP command does not take any arguments")
        return ('REP',)

    else:
        return tuple(parts)
# END parse_command


def other_request(
    endpoint: str,
):
    """
    Send a request to the other endpoint.
    """

    request_id = random.randint(100000, 999999)
    print(request_id)

    payload = {
        'request_id': request_id
    }

    response = requests.get(f'{url}/{endpoint}', json=payload)

    return f'Response: {response.text}Code: {response.status_code}'
# END other_request


def call_endpoint(endpoint: str, *args):
    if endpoint == 'ADD':
        return add_request(*args)
    elif endpoint == 'DEL':
        return delete_request(*args)
    elif endpoint == 'REP':
        return view_request()
    else:
        return other_request(endpoint)
    

# END call_endpoint


def main():
    while True:
        cmd = input("Enter command: ")
        try:
            endpoint, *args = parse_command(cmd)
            print(call_endpoint(endpoint, *args))
        except ValueError as e:
            print(e)
# END main


if __name__ == '__main__':
    main()
