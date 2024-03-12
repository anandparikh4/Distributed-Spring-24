from quart import Blueprint, jsonify, request

from utils import *

blueprint = Blueprint('add', __name__)


@blueprint.route('/add', methods=['POST'])
async def add():
    """
    Add new server replica hostname(s) and shards to the list.

    If `len(servers) == n`:
        Update the `serv_ids` with the new server ids.
        Update the `replicas` with the new hostnames.
        Update the `shard_map` with the new shard -> server mapping.
    If `new_shards` is not empty:
        Add the new shards to the `shard_map`.
        Add the new shards to the `shard_locks`.
        Add the new shards to the `shardT` table.

    If `len(servers) != n`:
        Return an error message.
    If relevant fields are not present in `new_shards`:
        Return an error message.
    If `n > remaining slots`:
        Return an error message.
    If `hostname` in `servers` already exists in `replicas`:
        Do not add any replicas to the list.
        Return an error message.
    If some shard in `new_shards` already exists in `shard_map`:
        Return an error message.
    If some shard for some server in `servers` does not exist in `shard_map` union `new_shards`:
        Return an error message.

    `Request payload:`
        `n: number of servers to add`
        `new_shards: list of new shard names and description to add [optional]`
            `stud_id_low: lower bound of student id`
            `shard_id: name of the shard`
            `shard_size: size of the shard`
        `servers: dict of server hostname -> list of shard names to add [new shard names must be define in `new_shards`]`

    `Response payload:`
        `message:`
            `N: number of replicas`
            `replicas: list of replica servers`
        `status: status of the request`

    `Error payload:`
        `message: error message`
        `status: status of the request`
    """

    global replicas
    global heartbeat_fail_count
    global serv_ids
    global shard_map
    global pool

    # Allow other tasks to run
    await asyncio.sleep(0)

    try:
        # Get the request payload
        payload = dict(await request.get_json())
        ic(payload)

        if payload is None:
            raise Exception('Payload is empty')

        # Get the number of servers to add
        n = int(payload.get('n', -1))

        # Get the list of server replica hostnames to add
        servers: Dict[str, List[str]] = dict(payload.get('servers', {}))
        hostnames = list(servers.keys())

        new_shards: List[Dict[str, Any]] = list(payload.get('new_shards', []))

        if len(hostnames) != n:
            raise Exception(
                'Length of server list is not equal to instances to add')

        # Check if relevant fields are present
        for shard in new_shards:
            if not all(k in shard.keys()
                       for k in
                       ('stud_id_low', 'shard_id', 'shard_size')):
                raise Exception('Invalid shard description')
        # END for shard in new_shards

        async with lock(Write):
            # Check is slots are available
            if n > replicas.remaining():
                raise Exception(
                    f'Insufficient slots. Only {replicas.remaining()} slots left')

            hostnames_set = set(hostnames)
            replicas_set = set(replicas.getServerList())
            new_shard_ids: Set[str] = set(shard['shard_id']
                                          for shard in new_shards)

            # Check if all `hostnames` are not in `replicas`
            if not hostnames_set.isdisjoint(replicas_set):
                raise Exception(
                    f'Hostnames `{hostnames_set & replicas_set}` are already in replicas')

            # Check if all `new_shards` are not in `shard_map`
            if not new_shard_ids.isdisjoint(shard_map.keys()):
                raise Exception(
                    f'Shards `{new_shard_ids & shard_map.keys()}` are already in shard_map')

            # Check if all shards for all servers in `hostnames` are in `shard_map` union `new_shards`
            problems = set()
            for shards in servers.values():
                problems |= set(shards) - new_shard_ids - shard_map.keys()
            # END for shards in servers.values()

            if len(problems) > 0:
                raise Exception(
                    f'Shards `{problems}` are not defined in shard_map union new_shards')

            ic("To add: ", hostnames, new_shards)

            # Spawn new containers
            semaphore = asyncio.Semaphore(DOCKER_TASK_BATCH_SIZE)

            async with Docker() as docker:
                # Define tasks
                tasks = []

                # Add the hostnames to the list
                for hostname in hostnames:
                    # Add the hostname to the replicas list
                    replicas.add(hostname)

                    # Edit the flatline map
                    heartbeat_fail_count[hostname] = 0

                    # get new server id
                    serv_id = get_new_server_id()
                    serv_ids[hostname] = serv_id

                    # Add the shards to the shard_locks and shard_map
                    for shard in new_shard_ids:
                        # Change to ConsistentHashMap
                        shard_map[shard] = []
                        shard_locks[shard] = FifoLock()
                    # END for shard in new_shards

                    tasks.append(spawn_container(docker, serv_id,
                                                 hostname, semaphore))
                # END for hostname in hostnames

                # Wait for all tasks to complete
                await asyncio.gather(*tasks, return_exceptions=True)
            # END async with Docker

            # Copy shards to the new containers
            semaphore = asyncio.Semaphore(REQUEST_BATCH_SIZE)

            # Define tasks
            tasks = [asyncio.create_task(
                copy_shards_to_container(hostname,
                                         servers[hostname],
                                         semaphore)
                     for hostname in hostnames)]

            # Wait for all tasks to complete
            await asyncio.gather(*tasks, return_exceptions=True)

            # Update the shard_map with the new replicas
            for hostname in hostnames:
                for shard in servers[hostname]:
                    shard_map[shard].append(hostname)
                # END for shard in servers[hostname]
            # END for hostname in hostnames

            async with pool.acquire() as conn:
                stmt = await conn.prepare(
                    '''--sql
                    INSERT INTO shardT (
                        stud_id_low,
                        shard_id,
                        shard_size)
                    VALUES (
                        $1::INTEGER,
                        $2::TEXT,
                        $3::INTEGER)
                    ''')

                async with conn.transaction():
                    await stmt.executemany(
                        [(shard['stud_id_low'],
                          shard['shard_id'],
                          shard['shard_size'])
                         for shard in new_shards])
                # END async with conn.transaction()
            # END async with pool.acquire() as conn

            final_hostnames = ic(replicas.getServerList())
        # END async with lock(Write)

        # Return the response payload
        return jsonify(ic({
            'message': {
                'N': len(replicas),
                'replicas': final_hostnames,
            },
            'status': 'success'
        })), 200

    except Exception as e:
        if DEBUG:
            print(f'{Fore.RED}ERROR | '
                  f'{e}'
                  f'{Style.RESET_ALL}',
                  file=sys.stderr)

        return jsonify(ic(err_payload(e))), 400
    # END try-except
# END add


async def spawn_container(
    docker: Docker,
    serv_id: int,
    hostname: str,
    semaphore: asyncio.Semaphore
):
    # Allow other tasks to run
    await asyncio.sleep(0)

    try:
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

    except Exception as e:
        if DEBUG:
            print(f'{Fore.RED}ERROR | '
                  f'{e}'
                  f'{Style.RESET_ALL}',
                  file=sys.stderr)
    # END try-except
# END spawn_container


async def copy_shards_to_container(
    hostname: str,
    shards: List[str],
    semaphore: asyncio.Semaphore
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

    try:
        # List of shards to copy from each server A [server -> list of [shard_id, valid_at]]
        call_server_shards: Dict[str, List[Tuple[str, int]]] = {}

        # For each shard K in `shards`:
        async with pool.acquire() as conn:
            stmt = await conn.prepare(
                '''--sql
                SELECT
                    valid_at
                FROM
                    ShardT
                WHERE
                    shard_id = $1::TEXT
                ''')

            async with conn.transaction():
                for shard in shards:
                    # Ignore empty shards
                    if len(shard_map[shard]) == 0:
                        continue

                    # Get server A from `shard_map` for the shard K
                    # TODO: Chage to ConsistentHashMap
                    server = shard_map[shard][0]

                    shard_valid_at: int = await stmt.fetchval(shard)

                    if server not in call_server_shards:
                        call_server_shards[server] = []

                    call_server_shards[server].append((shard, shard_valid_at))
                # END for shard in shards
            # END async with conn.transaction()
        # END async with pool.acquire() as conn

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

    except Exception as e:
        if DEBUG:
            print(f'{Fore.RED}ERROR | '
                  f'{e}'
                  f'{Style.RESET_ALL}',
                  file=sys.stderr)
    # END try-except

# END copy_shards_to_container
