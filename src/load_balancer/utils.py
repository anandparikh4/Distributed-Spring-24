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


def get_container_config(
    serv_id: int,
    hostname: str
):
    """
    Get the container config for the server replica.
    """

    return {
        'image': 'server:v2',
        'detach': True,
        'env': [
            f'SERVER_ID={serv_id:06}',
            'DEBUG=true',
            'POSTGRES_HOST=localhost',
            'POSTGRES_PORT=5432',
            'POSTGRES_USER=postgres',
            'POSTGRES_PASSWORD=postgres',
            'POSTGRES_DB_NAME=postgres',
        ],
        'hostname': hostname,
        'tty': True,
    }


async def add_shards(
    hostname: str,
    shards: list[str]
):
    """
    Call the /config endpoint of the server replica to add shards.

    `hostname:` server replica hostname
    `shards:` list of shard names to add
    """

    # To allow other tasks to run
    await asyncio.sleep(0)

    async def server_config_post_wrapper(
        session: aiohttp.ClientSession,
        server_name: str,
        payload: dict
    ):
        # To allow other tasks to run
        await asyncio.sleep(0)

        async with session.post(f'http://{server_name}:5000/config',
                                json=payload) as response:
            await response.read()

        return response
    # END server_config_post_wrapper

    # Convert to aiohttp request
    timeout = aiohttp.ClientTimeout(connect=REQUEST_TIMEOUT)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        payload = {'shards': shards}
        tasks = [asyncio.create_task(
            server_config_post_wrapper(session, hostname, payload))]
        serv_response = await asyncio.gather(*tasks, return_exceptions=True)
        serv_response = serv_response[0] if not isinstance(
            serv_response[0], BaseException) else None
    # END async with

    if serv_response is None:
        raise Exception('Server did not respond. Failed to add shards')

# END add_shards


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
            container_config = get_container_config(serv_id, hostname)

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

        # Call /config endpoint to add shards to the server
        shards = [shard
                  for shard, servers in shard_map.items()
                  if hostname in servers]

        await add_shards(hostname, shards)

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
    global serv_ids

    if DEBUG:
        print(f'{Fore.CYAN}HEARTBEAT | '
              'Heartbeat background task started'
              f'{Style.RESET_ALL}',
              file=sys.stderr)

    await asyncio.sleep(0)

    try:
        while True:
            # check heartbeat every `HEARTBEAT_INTERVAL` seconds
            await asyncio.sleep(HEARTBEAT_INTERVAL)

            async with lock(Read):
                if DEBUG:
                    print(f'{Fore.CYAN}HEARTBEAT | '
                          f'Checking heartbeat every {HEARTBEAT_INTERVAL} seconds'
                          f'{Style.RESET_ALL}',
                          file=sys.stderr)

                # Get the list of server replica hostnames
                hostnames = replicas.getServerList().copy()
            # END async with lock(Read)

            semaphore = asyncio.Semaphore(REQUEST_BATCH_SIZE)

            async def collect_heartbeat(
                session: aiohttp.ClientSession,
                server_name: str,
            ):

                # Allow other tasks to run
                await asyncio.sleep(0)

                async with semaphore:
                    async with session.get(f'http://{server_name}:5000/heartbeat') as response:
                        await response.read()

                    return response
                # END async with semaphore
            # END collect_heartbeats

            # Convert to aiohttp request
            timeout = aiohttp.ClientTimeout(connect=REQUEST_TIMEOUT)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                tasks = [asyncio.create_task(collect_heartbeat(session, server_name))
                         for server_name in hostnames]
                heartbeats = await asyncio.gather(*tasks, return_exceptions=True)
                heartbeats = [None if isinstance(heartbeat, BaseException)
                              else heartbeat for heartbeat in heartbeats]
            # END async with session

            # To allow other tasks to run
            await asyncio.sleep(0)

            semaphore = asyncio.Semaphore(DOCKER_TASK_BATCH_SIZE)

            async def handle_flatline_wrapper(
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
            # END handle_flatline_wrapper

            flatlines = []

            async with lock(Write):
                for i, response in enumerate(heartbeats):
                    if response is None or not response.status == 200:
                        # Increment the fail count for the server replica
                        heartbeat_fail_count[hostnames[i]] = \
                            heartbeat_fail_count.get(hostnames[i], 0) + 1

                        # If fail count exceeds the max count, respawn the server replica
                        if heartbeat_fail_count[hostnames[i]] >= MAX_FAIL_COUNT:
                            serv_id = serv_ids[hostnames[i]]
                            flatlines.append(
                                handle_flatline_wrapper(serv_id, hostnames[i]))
                    else:
                        # Reset the fail count for the server replica
                        heartbeat_fail_count[hostnames[i]] = 0
                    # END if-else
                # END for

                ic(heartbeat_fail_count)

                # Reswapn the flatlined server replicas
                if len(flatlines) > 0:
                    await asyncio.gather(*flatlines, return_exceptions=True)
            # END async with lock(Write)

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
        print(f'{Fore.RED}ERROR | '
              f'Failed to create database pool for {DB_NAME} at {DB_HOST}:{DB_PORT}'
              f'{Style.RESET_ALL}',
              file=sys.stderr)
        sys.exit(1)
    else:
        print(f'{Fore.LIGHTGREEN_EX}DB | '
              f'Created database pool for {DB_NAME} at {DB_HOST}:{DB_PORT}'
              f'{Style.RESET_ALL}',
              file=sys.stderr)

    return pool
# END create_db_pool


def get_new_server_id():
    """
    Get a new server id.
    """

    global serv_ids

    # generate new 6-digit id not in `serv_ids`
    new_id = random.randint(1, 999999)

    while new_id in serv_ids.values():
        new_id = random.randint(1, 999999)
    # END while

    return new_id
# END get_new_server_id
