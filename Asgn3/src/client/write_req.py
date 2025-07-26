import asyncio
import json
import aiohttp
import csv
import random
import time
from pprint import pp

N = 10000
len_data = 4096*4
doubles = len_data - N
singles = N - doubles
url = 'http://localhost:5000/write'


async def main():

    i = 0
    data = []
    with open('data/people.csv', 'r') as file:
        next(file)        # Skip header
        for row in csv.reader(file):
            data.append(row)
            i += 1
            if i == len_data:
                break

    random.shuffle(data)

    data = [{
        'stud_id': int(row[0]),
        'stud_name': row[1],
        'stud_marks': int(row[2])
    } for row in data]

    BATCH = 100
    semaphore = asyncio.Semaphore(BATCH)

    async def wrapper(payload: dict):
        async with semaphore:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as resp:
                    await resp.read()
                return resp.status

    tasks = []
    cnt = 0
    i = 0
    while i < len_data:
        if cnt < doubles:
            tasks.append(asyncio.create_task(
                wrapper({"data": [data[i], data[i+1]]})))
            i += 2
            cnt += 1
        else:
            tasks.append(asyncio.create_task(wrapper({"data": [data[i]]})))
            i += 1

    print(f"Total requests: {len(tasks)}")

    status = await asyncio.gather(*tasks, return_exceptions=True)

    for i, s in enumerate(status):
        if isinstance(s, BaseException):
            print(f"Error: {s}")
            print()
        elif s != 200:
            print(f"Status: {s}")
            print()

if __name__ == "__main__":
    start = time.perf_counter()
    asyncio.run(main())
    end = time.perf_counter()

    print(f"Time taken: {end-start:.2f} seconds")
