import asyncio
from pprint import pp
import time
from typing import List, Tuple
import random
import aiohttp

# URL of the server
url = 'http://127.0.0.1:5000/read'


async def read():
    ranges: List[Tuple[int, int]] = []

    # Generate ranges
    N = 10000
    MAX = 10000

    for _ in range(N):
        low = random.randint(0, MAX)
        high = random.randint(low, MAX)

        ranges.append((low, high))

    # pp(ranges)

    BATCH = 100
    semaphore = asyncio.Semaphore(BATCH)

    async def wrapper(payload: dict):
        async with semaphore:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, json=payload) as resp:
                    await resp.read()
                return resp.status

    tasks = [asyncio.create_task(
        wrapper(
            payload={
                "stud_id": {
                    "low": r[0],
                    "high": r[1]
                }
            }
        )) for r in ranges]

    status = await asyncio.gather(*tasks, return_exceptions=True)

    for i, s in enumerate(status):
        if isinstance(s, BaseException):
            print(f"Error: {s}")
            print()
        elif s != 200:
            print(f"Status: {s}")
            print()


# END main


if __name__ == '__main__':
    start = time.perf_counter()
    asyncio.run(read())
    end = time.perf_counter()

    print(f"Time taken: {end-start:.2f} seconds")
