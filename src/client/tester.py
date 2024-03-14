import asyncio
import sys
from pprint import pp
from time import time

import aiohttp
from matplotlib import pyplot as plt

# URL of the server
url = 'http://127.0.0.1:5000'


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
            async with session.get(url) as response:
                await response.read()

            await asyncio.sleep(0)

            return response
    # END fetch

    tasks = [fetch(url) for url in urls]

    return [None if isinstance(r, BaseException)
            else r for r in
            await asyncio.gather(*tasks, return_exceptions=True)]


async def main(server_count: int = 3):
    async with aiohttp.ClientSession() as session:
        payload = {
            'n': 3,
            'shards': [{'shard_id': 'sh1', 'stud_id_low': 0, 'shard_size': 5}],
            'servers': {'server0': ['sh1'], 'server1': ['sh1'], 'server2': ['sh1']}
        }

        response = await session.post(f'{url}/init', json=payload)
        pp(await response.json())
    #     responses = await gather_with_concurrency(
    #         session, 1000, *[f'{url}/home' for _ in range(10_000)])

    # N = server_count
    # counts = {k: 0 for k in range(N+1)}

    # # for _, response in grequests.imap_enumerated(requests):
    # for response in responses:
    #     if response is None or not response.status == 200:
    #         counts[0] += 1
    #         continue

    #     payload: dict = await response.json()
    #     msg = payload.get('message', '')
    #     server_id = int(msg.split(':')[-1].strip())
    #     counts[server_id] += 1

    # pp(counts)
    # pp(sum(counts.values()))
    # plt.bar(list(counts.keys()), list(counts.values()))
    # plt.savefig(f'../../plots/plot-{N}-{int(time()*1e3)}.jpg')
    # plt.show()


# END main

if __name__ == '__main__':
    num = int(sys.argv[1]) if len(sys.argv) > 1 else 3

    asyncio.run(main(num))
