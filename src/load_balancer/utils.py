import asyncio
import time
import random


class Read(asyncio.Future):
    @staticmethod
    def is_compatible(holds):
        return not holds[Write]


class Write(asyncio.Future):
    @staticmethod
    def is_compatible(holds):
        return not holds[Read] and not holds[Write]


def random_hostname():
    """
    Generate a random hostname.
    """

    return f'Server-{random.randint(0, 10000):04}-{int(time.time()*1e3)}'
# END random_hostname
