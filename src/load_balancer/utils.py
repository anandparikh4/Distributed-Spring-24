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

    return f'Server-{random.randint(0, 10000):04}-{int(time.time()*1e3)}'
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


async def my_gather(*tasks, return_exceptions=True):
    """
    Gather with concurrency from aiohttp async session
    """

    res = await asyncio.gather(*tasks, return_exceptions=return_exceptions)
    return [None if isinstance(r, BaseException) else r for r in res]
# END gather_with_concurrency


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

    return await my_gather(*[fetch(url) for url in urls],
                           return_exceptions=True)


# END gather_with_concurrency
