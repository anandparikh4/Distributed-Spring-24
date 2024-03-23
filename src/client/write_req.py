import asyncio
import json
import aiohttp
import csv
import random
import time
from pprint import pp

N = 10000
url = 'http://localhost:5000/write'

async def main():

    i = 0
    data = []
    with open('data/people.csv', 'r') as file:
        next(file)        # Skip header
        for row in csv.reader(file):
            data.append(row)
            i += 1
            if i == N:
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

    tasks = []
    for item in data:
        tasks.append(asyncio.create_task(wrapper({"data": [item]})))

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    start = time.perf_counter()
    asyncio.run(main())
    end = time.perf_counter()

    print(f"Time taken: {end-start:.2f} seconds")
