import asyncio
import json
import aiohttp
import csv
import random
import time
from pprint import pp


async def write_db(data: dict):
    async with aiohttp.ClientSession() as session:
        async with session.post('http://localhost:5000/write',
                                json=data) as resp:
            print(resp.status)
            print(await resp.json())


async def main():
    # csv to dict
    with open('data/people.csv', 'r') as file:
        reader = csv.DictReader(file)
        data = list(reader)

    data = [{
        'stud_id': int(row['id']),
        'stud_name': row['name'],
        'stud_marks': int(row['marks'])
    } for row in data[:4096*4]]

    # data = random.sample(data, 4096*4)
    await write_db({"data": data})

if __name__ == "__main__":
    start = time.perf_counter()
    asyncio.run(main())
    end = time.perf_counter()

    print(f"Time taken: {end-start:.2f} seconds")
