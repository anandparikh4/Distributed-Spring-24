from common import *
import common

class Read(asyncio.Future):
    @staticmethod
    def is_compatible(holds):
        return not holds[Write]
# END class Read


class Write(asyncio.Future):
    @staticmethod
    def is_compatible(holds):
        return not holds[Read] and not holds[Write]
# END class Write


def random_hostname():
    """
    Generate a random hostname.
    """

    return f'Server-{random.randrange(0, 1000):03}-{int(time.time()*1e3) % 1000:03}'
# END random_hostname


def err_payload(err: Exception):
    """
    Generate an error payload.
    """
    
    if DEBUG:
        print(f'{Fore.RED}ERROR | '
              f'{err.__class__.__name__}\n'
              f'{err}\n'
              f'{err.__traceback__}\n'
              f'{Style.RESET_ALL}',
              file=sys.stderr)

    return {
        'message': f'<Error> {err.__class__.__name__}: {err}',
        'status': 'failure'
    }
# END err_payload


def get_container_config(
    serv_id: int,
    hostname: str
):
    """
    Get the container config for the server replica.
    """

    return {
        'image': 'server:v2',
        'detach': True,
        'env': [
            f'SERVER_ID={serv_id:06}',
            'DEBUG=true',
            'POSTGRES_HOST=localhost',
            'POSTGRES_PORT=5432',
            'POSTGRES_USER=postgres',
            'POSTGRES_PASSWORD=postgres',
            'POSTGRES_DB_NAME=postgres',
        ],
        'hostname': hostname,
        'tty': True,
    }


def get_new_server_id():
    """
    Get a new server id.
    """

    global serv_ids

    # generate new 6-digit id not in `serv_ids`
    new_id = get_request_id()

    while new_id in serv_ids.values():
        new_id = get_request_id()
    # END while

    return new_id
# END get_new_server_id


def get_request_id():
    """
    Get a new request id.
    """

    return random.randint(100000, 999999)
# END get_request_id
