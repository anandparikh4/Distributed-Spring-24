from utils import *


async def spawn_container(
    docker: Docker,
    serv_id: int,
    hostname: str,
    semaphore: asyncio.Semaphore
):
    # Allow other tasks to run
    await asyncio.sleep(0)

    async with semaphore:
        # spawn new docker containers for the new hostnames
        container_config = \
            get_container_config(serv_id, hostname)

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
            print(f'{Fore.LIGHTGREEN_EX}CREATE | '
                  f'Created container for {hostname}'
                  f'{Style.RESET_ALL}',
                  file=sys.stderr)

        # start the container
        await container.start()

        if DEBUG:
            print(f'{Fore.MAGENTA}SPAWN | '
                  f'Started container for {hostname}'
                  f'{Style.RESET_ALL}',
                  file=sys.stderr)

    # END async with semaphore
# END spawn_container


async def copy_shards_to_container(
    hostname: str,
    shards: List[str],
    semaphore: asyncio.Semaphore,
    servers_flatlined: Optional[List[str]]=None
):
    """
    1. Call /config endpoint on the server S with the hostname
    1. For each shard K in `shards`:
        1. Get server A from `shard_map` for the shard K
        1. Call /copy on server A to copy the shard K
        1. Call /write on server S to write the shard K

    Args:
        - hostname: hostname of the server
        - shards: list of shard names to copy
        - semaphore: asyncio.Semaphore
    """

    if servers_flatlined is None:
        servers_flatlined = []

    global shard_map

    # Allow other tasks to run
    await asyncio.sleep(0)

    async def post_config_wrapper(
        session: aiohttp.ClientSession,
        hostname: str,
        payload: Dict,
    ):
        # Allow other tasks to run
        await asyncio.sleep(0)

        async with semaphore:
            # Wait for the server to be up
            for _ in range(MAX_CONFIG_FAIL_COUNT):
                try:
                    async with session.get(f'http://{hostname}:5000/heartbeat') as response:
                        if response.status == 200:
                            break
                except Exception:
                    pass
                await asyncio.sleep(HEARTBEAT_CONFIG_INTERVAL)
            else:
                raise Exception()
            # END for _ in range(MAX_CONFIG_FAIL_COUNT)

            async with session.post(f'http://{hostname}:5000/config',
                                    json=payload) as response:
                await response.read()

            return response
        # END async with semaphore
    # END post_config_wrapper

    async def get_copy_wrapper(
        session: aiohttp.ClientSession,
        hostname: str,
        payload: Dict,
    ):
        # Allow other tasks to run
        await asyncio.sleep(0)

        async with semaphore:
            async with session.get(f'http://{hostname}:5000/copy',
                                   json=payload) as response:
                await response.read()

            return response
        # END async with semaphore
    # END get_copy_wrapper

    async def post_write_wrapper(
        session: aiohttp.ClientSession,
        hostname: str,
        payload: Dict,
    ):
        # Allow other tasks to run
        await asyncio.sleep(0)

        async with semaphore:
            async with session.post(f'http://{hostname}:5000/write',
                                    json=payload) as response:
                await response.read()

            return response
        # END async with semaphore

    # List of shards to copy from each server A [server -> list of [shard_id, valid_at]]
    call_server_shards: Dict[str, List[Tuple[str, int]]] = {}

    # For each shard K in `shards`:
    async with common.pool.acquire() as conn:
        async with conn.transaction():
            stmt = await conn.prepare(
                '''--sql
                SELECT
                    valid_at
                FROM
                    ShardT
                WHERE
                    shard_id = $1::TEXT;
                ''')

            for shard in shards:
                if len(shard_map[shard]) == 0:
                    continue

                # Get server A from `shard_map` for the shard K
                # TODO: Chage to ConsistentHashMap

                server = shard_map[shard].find(get_request_id())
                if len(servers_flatlined) > 0:
                    while server in servers_flatlined:
                        server = shard_map[shard].find(get_request_id())

                shard_valid_at: int = await stmt.fetchval(shard)

                if server not in call_server_shards:
                    call_server_shards[server] = []

                call_server_shards[server].append((shard, shard_valid_at))
            # END for shard in shards
        # END async with conn.transaction()
    # END async with common.pool.acquire() as conn
    
    ic(call_server_shards)

    timeout = aiohttp.ClientTimeout(connect=REQUEST_TIMEOUT)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # Call /config endpoint on the server S with the hostname
        config_task = asyncio.create_task(
            post_config_wrapper(
                session,
                hostname,
                payload={
                    "shards": shards
                }
            )
        )
        config_response = await asyncio.gather(*[config_task], return_exceptions=True)
        config_response = (None if isinstance(config_response[0], BaseException)
                           else config_response[0])

        if config_response is None or config_response.status != 200:
            raise Exception(f'Failed to add shards to {hostname}')

        # Call /copy on server A to copy the shard K
        # Define tasks
        tasks = [asyncio.create_task(
            get_copy_wrapper(
                session,
                server,
                payload={
                    "shards": [shard[0] for shard in shards],
                    "valid_at": [shard[1] for shard in shards],
                }
            )
        ) for server, shards in call_server_shards.items()]

        # Wait for all tasks to complete
        copy_responses = await asyncio.gather(*tasks, return_exceptions=True)
        copy_responses = [None if isinstance(response, BaseException)
                          else response
                          for response in copy_responses]

        # Get the data from the copy_responses [shard_id -> (list of data, valid_at)]
        all_data: Dict[str, Tuple[List, int]] = {}

        for (response, server_shards) in zip(copy_responses,
                                             call_server_shards.values()):
            if response is None or response.status != 200:
                raise Exception(f'Failed to copy shards to {hostname}')

            data: Dict = await response.json()

            for shard_id, valid_at in server_shards:
                all_data[shard_id] = (data[shard_id], valid_at)
        # END for (response, shards) in zip(copy_responses, call_server_shards.values())
        
        ic(all_data)

        # Call /write on server S to write the shard K
        # Define tasks
        tasks = [asyncio.create_task(
            post_write_wrapper(
                session,
                hostname,
                payload={
                    'shard': shard,
                    'data': data,
                    'admin': True,
                    'valid_at': valid_at,
                }
            )
        ) for shard, (data, valid_at) in all_data.items()]

        # Wait for all tasks to complete
        write_responses = await asyncio.gather(*tasks, return_exceptions=True)
        write_responses = [None if isinstance(response, BaseException)
                           else response
                           for response in write_responses]

        if any(response is None or response.status != 200
                for response in write_responses):
            raise Exception(f'Failed to write shards to {hostname}')

    # END async with aiohttp.ClientSession
# END copy_shards_to_container
