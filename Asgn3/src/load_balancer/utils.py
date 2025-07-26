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


def err_payload(err: Exception):
    """
    Generate an error payload.
    """

    if DEBUG:
        print(f'{Fore.RED}ERROR | '
              f'{err.__class__.__name__}: {err}\n'
              f'{Style.RESET_ALL}',
              file=sys.stderr)

    return {
        'message': f'<Error> {err.__class__.__name__}: {err}',
        'status': 'failure'
    }
# END err_payload


def get_request_id():
    """
    Get a new request id.
    """

    return random.randint(100000, 999999)
# END get_request_id
