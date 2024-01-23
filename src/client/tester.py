import sys
import signal
import random
# import grequests
from matplotlib import pyplot as plt
from pprint import pp
import asyncio
import aiohttp

# %matplotlib inline

# URL of the server
url = 'http://127.0.0.1:5000'
endpoints = ['add', 'rm', 'rep']

# clean exit on ctrl-c


def signal_handler(sig, frame):
    print('Exiting...')
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)


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

    # response = requests.get(f'{url}/{endpoint}', json=payload)
    request = grequests.get(f'{url}/{endpoint}', json=payload)
    response = grequests.map([request])[0]

    return f'Response: {response.text}Code: {response.status_code}'
# END other_request

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


async def main():

    # requests = [grequests.get(f'{url}/home') for _ in range(500)]
    # responses = grequests.map(requests)

    # N = 3
    # counts = {k: 0 for k in range(N+1)}

    # # for _, response in grequests.imap_enumerated(requests):
    # for response in responses:
    #     if response is None or not response.status_code == 200:
    #         counts[0] += 1
    #         continue

    #     payload = response.json()
    #     msg = payload['message']
    #     server_id = int(msg.split(':')[-1].strip())
    #     counts[server_id] += 1

    # pp(counts)
    # plt.bar(list(counts.keys()), list(counts.values()))
    # plt.savefig('plot.jpg')

    async with aiohttp.ClientSession() as session:
        responses = await gather_with_concurrency(session, 100, *[f'{url}/home' for _ in range(100)])

    N = 3
    counts = {k: 0 for k in range(N+1)}

    # for _, response in grequests.imap_enumerated(requests):
    for response in responses:
        if response is None or not response.status == 200:
            counts[0] += 1
            continue

        payload = response.json()
        msg = payload['message']
        server_id = int(msg.split(':')[-1].strip())
        counts[server_id] += 1

    pp(counts)
    plt.bar(list(counts.keys()), list(counts.values()))
    plt.savefig('plot.jpg')


# END main

if __name__ == '__main__':
    asyncio.run(main())
