import asyncio
import random
import time

import aiohttp
import asyncpg

from common import *


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

    # Allow other tasks to run
    await asyncio.sleep(0)

    semaphore = asyncio.Semaphore(batch)

    async def fetch(url: str):
        # Allow other tasks to run
        await asyncio.sleep(0)

        async with semaphore:
            async with session.get(url) as response:
                await response.read()

            return response
        # END async with semaphore
    # END fetch

    tasks = [fetch(url) for url in urls]

    return [None if isinstance(r, BaseException)
            else r for r in
            await asyncio.gather(*tasks, return_exceptions=True)]
# END gather_with_concurrency


async def handle_flatline(
    serv_id: int,
    hostname: str
):
    """
    Handles the flatline of a server replica.
    """

    # Allow other tasks to run
    await asyncio.sleep(0)

    if DEBUG:
        print(f'{Fore.LIGHTRED_EX}FLATLINE | '
              f'Flatline of server replica `{hostname}` detected'
              f'{Style.RESET_ALL}',
              file=sys.stderr)

    try:
        async with Docker() as docker:
            container_config = {
                'image': 'server:v1',
                'detach': True,
                'env': [f'SERVER_ID={serv_id}',
                        'DEBUG=true'],
                'hostname': hostname,
                'tty': True,
            }

            # create the container
            container = await docker.containers.create_or_replace(
                name=hostname,
                config=container_config,
            )

            # Attach the container to the network and set the alias
            my_net = await docker.networks.get('my_net')

            await my_net.connect({
                'Container': container.id,
                'EndpointConfig': {
                    'Aliases': [hostname]
                }
            })

            if DEBUG:
                print(f'{Fore.LIGHTGREEN_EX}RECREATE | '
                      f'Created container for {hostname}'
                      f'{Style.RESET_ALL}',
                      file=sys.stderr)

            # start the container
            await container.start()

            if DEBUG:
                print(f'{Fore.MAGENTA}RESPAWN | '
                      f'Started container for {hostname}'
                      f'{Style.RESET_ALL}',
                      file=sys.stderr)
        # END async with docker

    except Exception as e:
        if DEBUG:
            print(f'{Fore.RED}ERROR | '
                  f'{e}'
                  f'{Style.RESET_ALL}',
                  file=sys.stderr)
    # END try-except
# END handle_flatline


async def get_heartbeats():
    """
    Calls the heartbeat endpoint of all the replicas.
    If a replica does not respond, it is respawned.
    """

    global replicas
    global heartbeat_fail_count
    global serv_id

    if DEBUG:
        print(f'{Fore.CYAN}HEARTBEAT | '
              'Heartbeat background task started'
              f'{Style.RESET_ALL}',
              file=sys.stderr)

    await asyncio.sleep(0)

    try:
        while True:
            async with lock(Read):
                if DEBUG:
                    print(f'{Fore.CYAN}HEARTBEAT | '
                          f'Checking heartbeat every {HEARTBEAT_INTERVAL} seconds'
                          f'{Style.RESET_ALL}',
                          file=sys.stderr)

                # Get the list of server replica hostnames
                hostnames = replicas.getServerList().copy()
            # END async with lock(Read)

            # Generate heartbeat urls
            heartbeat_urls = [f'http://{server_name}:5000/heartbeat'
                              for server_name in hostnames]

            # Convert to aiohttp request
            timeout = aiohttp.ClientTimeout(connect=REQUEST_TIMEOUT)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                heartbeats = await gather_with_concurrency(
                    session, REQUEST_BATCH_SIZE, *heartbeat_urls)
            # END async with session

            # To allow other tasks to run
            await asyncio.sleep(0)

            semaphore = asyncio.Semaphore(DOCKER_TASK_BATCH_SIZE)

            async def wrapper(
                serv_id: int,
                server_name: str
            ):
                # To allow other tasks to run
                await asyncio.sleep(0)

                async with semaphore:
                    try:
                        await handle_flatline(serv_id, server_name)
                    except Exception as e:
                        if DEBUG:
                            print(f'{Fore.RED}ERROR | '
                                  f'{e}'
                                  f'{Style.RESET_ALL}',
                                  file=sys.stderr)
                    # END try-except
                # END async with semaphore
            # END wrapper

            flatlines = []

            async with lock(Write):
                for i, response in enumerate(heartbeats):
                    if response is None or not response.status == 200:
                        # Increment the fail count for the server replica
                        heartbeat_fail_count[hostnames[i]] = \
                            heartbeat_fail_count.get(hostnames[i], 0) + 1

                        # If fail count exceeds the max count, respawn the server replica
                        if heartbeat_fail_count[hostnames[i]] >= MAX_FAIL_COUNT:
                            serv_id += 1
                            flatlines.append(wrapper(serv_id, hostnames[i]))
                    else:
                        # Reset the fail count for the server replica
                        heartbeat_fail_count[hostnames[i]] = 0
                    # END if-else
                # END for

                ic(heartbeat_fail_count)

                # Don't gather with lock held for optimization
                if len(flatlines) > 0:
                    await asyncio.gather(*flatlines, return_exceptions=True)
            # END async with lock(Write)

            # check heartbeat every `HEARTBEAT_INTERVAL` seconds
            await asyncio.sleep(HEARTBEAT_INTERVAL)
        # END while

    except asyncio.CancelledError:
        if DEBUG:
            print(f'{Fore.CYAN}HEARTBEAT | '
                  'Heartbeat background task stopped'
                  f'{Style.RESET_ALL}',
                  file=sys.stderr)
    # END try-except
# END get_heartbeats


async def create_db_pool():
    pool = await asyncpg.create_pool(
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        host=DB_HOST,
        port=DB_PORT
    )

    if pool is None:
        print(f'Failed to connect to database at {DB_HOST}:{DB_PORT}',
              file=sys.stderr)
        sys.exit(1)

    return pool
# END create_db_pool
