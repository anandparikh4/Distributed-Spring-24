import aiohttp
import asyncio
import random
import time


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

    return {
        'message': f'<Error> {err}',
        'status': 'failure'
    }
# END err_payload


async def gather_with_concurrency(
    session: aiohttp.ClientSession,
    batch: int,
    *urls: str
):
    """
    Gather with concurrency from aiohttp async session
    """

    semaphore = asyncio.Semaphore(batch)

    async def fetch(url: str):
        async with semaphore:
            await asyncio.sleep(0)

            async with session.get(url) as response:
                await response.read()

            return response
    # END fetch

    return [None if isinstance(r, BaseException)
            else r for r in
            await asyncio.gather(*[fetch(url) for url in urls],
                                 return_exceptions=True)]
# END gather_with_concurrency
