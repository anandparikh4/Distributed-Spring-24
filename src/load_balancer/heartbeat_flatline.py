from endpoints.config.add_helper import *
from utils import *


async def handle_flatline(
    docker_semaphore: asyncio.Semaphore,
    docker: Docker,
    serv_id: int,
    hostname: str,
    servers_flatlined: List[str]
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

    async with docker_semaphore:
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
    # END async with docker_semaphore

    # Copy shards to the new containers
    shards = [shard
              for shard, servers in shard_map.items()
              if hostname in servers.getServerList()]

    req_semaphore = asyncio.Semaphore(REQUEST_BATCH_SIZE)

    # Define task to copy shards to the new container
    task = asyncio.create_task(
        copy_shards_to_container(
            hostname,
            shards,
            req_semaphore,
            servers_flatlined
        )
    )

    # Wait for task to complete
    await asyncio.gather(*[task], return_exceptions=True)
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

    async def collect_heartbeat(
        semaphore: asyncio.Semaphore,
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

    async def handle_flatline_wrapper(
        semaphore: asyncio.Semaphore,
        docker: Docker,
        serv_id: int,
        hostname: str,
        servers_flatlined: List[str]
    ):
        # To allow other tasks to run
        await asyncio.sleep(0)

        async with semaphore:
            try:
                await handle_flatline(
                    docker_semaphore=semaphore,
                    docker=docker,
                    serv_id=serv_id,
                    hostname=hostname,
                    servers_flatlined=servers_flatlined
                )
            except Exception as e:
                if DEBUG:
                    print(f'{Fore.RED}ERROR | '
                          f'{e.__class__.__name__}: {e}'
                          f'{Style.RESET_ALL}',
                          file=sys.stderr)
            # END try-except
        # END async with semaphore
    # END handle_flatline_wrapper

    try:
        while True:
            # check heartbeat every `HEARTBEAT_INTERVAL` seconds
            await asyncio.sleep(HEARTBEAT_INTERVAL)

            async with common.lock(Read):
                if DEBUG:
                    print(f'{Fore.CYAN}HEARTBEAT | '
                          f'Checking heartbeat every {HEARTBEAT_INTERVAL} seconds'
                          f'{Style.RESET_ALL}',
                          file=sys.stderr)

                # Get the list of server replica hostnames
                hostnames = replicas.getServerList().copy()
            # END async with common.lock(Read)

            semaphore = asyncio.Semaphore(REQUEST_BATCH_SIZE)
            # Convert to aiohttp request
            timeout = aiohttp.ClientTimeout(connect=REQUEST_TIMEOUT)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                tasks = [asyncio.create_task(
                    collect_heartbeat(
                        semaphore,
                        session,
                        server_name
                    )
                ) for server_name in hostnames]

                heartbeats = await asyncio.gather(*tasks, return_exceptions=True)
                heartbeats = [None if isinstance(heartbeat, BaseException)
                              else heartbeat
                              for heartbeat in heartbeats]
            # END async with session

            # To allow other tasks to run
            await asyncio.sleep(0)

            flatlines: List[Tuple[str, int]] = []

            async with common.lock(Write):
                for i, response in enumerate(heartbeats):
                    if response is None or not response.status == 200:
                        # Increment the fail count for the server replica
                        heartbeat_fail_count[hostnames[i]] = \
                            heartbeat_fail_count.get(hostnames[i], 0) + 1

                        # If fail count exceeds the max count, respawn the server replica
                        if heartbeat_fail_count[hostnames[i]] >= MAX_HEARTBEAT_FAIL_COUNT:
                            serv_id = serv_ids[hostnames[i]]
                            flatlines.append((hostnames[i], serv_id))
                    else:
                        # Reset the fail count for the server replica
                        heartbeat_fail_count[hostnames[i]] = 0
                    # END if-else
                # END for

                ic(heartbeat_fail_count)

                # Reswapn the flatlined server replicas
                if len(flatlines) > 0:
                    docker_semaphore = asyncio.Semaphore(
                        DOCKER_TASK_BATCH_SIZE)
                    
                    servers_flatlined = [name for name, _ in flatlines]

                    async with Docker() as docker:
                        tasks = [asyncio.create_task(
                            handle_flatline_wrapper(
                                semaphore=docker_semaphore,
                                docker=docker,
                                serv_id=serv_id,
                                hostname=serv_name,
                                servers_flatlined=servers_flatlined
                            )
                        ) for serv_name, serv_id in flatlines]

                        await asyncio.gather(*tasks, return_exceptions=True)
            # END async with common.lock(Write)

        # END while

    except asyncio.CancelledError:
        if DEBUG:
            print(f'{Fore.CYAN}HEARTBEAT | '
                  'Heartbeat background task stopped'
                  f'{Style.RESET_ALL}',
                  file=sys.stderr)
    # END try-except
# END get_heartbeats
