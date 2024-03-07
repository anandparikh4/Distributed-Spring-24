from typing import Any, Dict, List
from quart import Blueprint, current_app, jsonify, request

from utils import *

blueprint = Blueprint('add', __name__)


# @blueprint.route('/add_old', methods=['POST'])
# async def add_old():
#     """
#     Add new server replica hostname(s) to the list.

#     If `len(hostnames) <= n`:
#         Add `hostnames` and `n - len(hostnames)` random hostnames to the list.
#     If `n <= 0`:
#         Return an error message.
#     If `len(hostnames) > n`:
#         Return an error message.
#     If `hostnames` contains duplicates:
#         Return an error message.
#     If `n > remaining slots`:
#         Return an error message.
#     If `hostname` in `hostnames` already exists in `replicas`:
#         Do not add any replicas to the list.
#         Return an error message.

#     Random hostnames are generated using the `random_hostname()` function.

#     `Request payload:`
#         `n: number of servers to add`
#         `hostnames: list of server replica hostnames to add (<= n) [optional]`

#     `Response payload:`
#         `message:`
#             `N: number of replicas`
#             `replicas: list of replica hostnames`
#         `status: status of the request`

#     `Error payload:`
#         `message: error message`
#         `status: status of the request`
#     """

#     global replicas
#     global heartbeat_fail_count
#     global serv_ids

#     # Allow other tasks to run
#     await asyncio.sleep(0)

#     try:
#         # Get the request payload
#         payload: dict = await request.get_json()
#         ic(payload)

#         if payload is None:
#             raise Exception('Payload is empty')

#         # Get the number of servers to add
#         n = int(payload.get('n', -1))

#         # Get the list of server replica hostnames to add
#         hostnames: list[str] = list(payload.get('hostnames', []))

#         if n <= 0:
#             raise Exception(
#                 'Number of servers to add must be greater than 0')

#         if len(hostnames) > n:
#             raise Exception(
#                 'Length of hostname list is more than instances to add')

#         if len(hostnames) != len(set(hostnames)):
#             raise Exception('Hostname list contains duplicates')

#         # Generate `n - len(hostnames)` random hostnames
#         new_hostnames: set[str] = set()
#         while len(new_hostnames) < n - len(hostnames):
#             new_hostnames.add(random_hostname())

#         # Add `new_hostnames` to the list.
#         hostnames.extend(new_hostnames)

#         async with lock(Write):
#             # Check is slots are available
#             if n > replicas.remaining():
#                 raise Exception(
#                     f'Insufficient slots. Only {replicas.remaining()} slots left')

#             hostnames_set = set(hostnames)
#             replicas_set = set(replicas.getServerList())

#             # Check if all `hostnames` are in `replicas`
#             if not hostnames_set.isdisjoint(replicas_set):
#                 raise Exception(
#                     f'Hostnames `{hostnames_set & replicas_set}` are already in replicas')

#             ic("To add: ", hostnames)

#             semaphore = asyncio.Semaphore(DOCKER_TASK_BATCH_SIZE)

#             async def spawn_container(
#                 docker: Docker,
#                 serv_id: int,
#                 hostname: str
#             ):
#                 # Allow other tasks to run
#                 await asyncio.sleep(0)

#                 async with semaphore:
#                     try:
#                         # spawn new docker containers for the new hostnames
#                         container_config = {
#                             'image': 'server:v1',
#                             'detach': True,
#                             'env': [f'SERVER_ID={serv_id}',
#                                     'DEBUG=true'],  # TODO: add more envs
#                             'hostname': hostname,
#                             'tty': True,
#                         }

#                         # create the container
#                         container = await docker.containers.create_or_replace(
#                             name=hostname,
#                             config=container_config,
#                         )

#                         # Attach the container to the network and set the alias
#                         my_net = await docker.networks.get('my_net')
#                         await my_net.connect({
#                             'Container': container.id,
#                             'EndpointConfig': {
#                                 'Aliases': [hostname]
#                             }
#                         })

#                         if DEBUG:
#                             print(f'{Fore.LIGHTGREEN_EX}CREATE | '
#                                   f'Created container for {hostname}'
#                                   f'{Style.RESET_ALL}',
#                                   file=sys.stderr)

#                         # start the container
#                         await container.start()

#                         if DEBUG:
#                             print(f'{Fore.MAGENTA}SPAWN | '
#                                   f'Started container for {hostname}'
#                                   f'{Style.RESET_ALL}',
#                                   file=sys.stderr)

#                     except Exception as e:
#                         if DEBUG:
#                             print(f'{Fore.RED}ERROR | '
#                                   f'{e}'
#                                   f'{Style.RESET_ALL}',
#                                   file=sys.stderr)
#                     # END try-except
#                 # END async with semaphore
#             # END spawn_container

#             async with Docker() as docker:
#                 # Define tasks
#                 tasks = []

#                 # Add the hostnames to the list
#                 for hostname in hostnames:
#                     replicas.add(hostname)

#                     # Edit the flatline map
#                     heartbeat_fail_count[hostname] = 0

#                     # get new server id
#                     serv_id = get_new_server_id()
#                     serv_ids[hostname] = serv_id

#                     tasks.append(spawn_container(docker, serv_id, hostname))
#                 # END for

#                 # Wait for all tasks to complete
#                 await asyncio.gather(*tasks, return_exceptions=True)
#             # END async with Docker

#             final_hostnames = ic(replicas.getServerList())
#         # END async with lock(Write)

#         # Return the response payload
#         return jsonify(ic({
#             'message': {
#                 'N': len(replicas),
#                 'replicas': final_hostnames
#             },
#             'status': 'success'
#         })), 200

#     except Exception as e:
#         if DEBUG:
#             print(f'{Fore.RED}ERROR | '
#                   f'{e}'
#                   f'{Style.RESET_ALL}',
#                   file=sys.stderr)

#         return jsonify(ic(err_payload(e))), 400
#     # END try-except
# # END add_old


@blueprint.route('/add', methods=['POST'])
async def add():
    """
    Add new server replica hostname(s) and shards to the list.

    If `len(hostnames) == n`:
        Add `hostnames` to the list.
    If `new_shards` is not empty:
        Add the new shards to the `shard_map`.

    If `len(hostnames) != n`:
        Return an error message.
    If relevant fields are not present in `new_shards`:
        Return an error message.
    If `n > remaining slots`:
        Return an error message.
    If `hostname` in `hostnames` already exists in `replicas`:
        Do not add any replicas to the list.
        Return an error message.
    If some shard in `new_shards` already exists in `shard_map`:
        Return an error message.
    If some shard for some server in `hostnames` does not exist in `shard_map` union `new_shards`:
        Return an error message.

    `Request payload:`
        `n: number of hostnames to add`
        `new_shards: list of new shard names and description to add [optional]`
            `stud_id_low: lower bound of student id`
            `shard_id: name of the shard`
            `shard_size: size of the shard`
        `hostnames: dict of server hostname -> list of shard names to add [new shard names must be define in `new_shards`]`

    `Response payload:`
        `message:`
            `N: number of replicas`
            `replicas: list of replica hostnames`
        `status: status of the request`

    `Error payload:`
        `message: error message`
        `status: status of the request`
    """

    global replicas
    global heartbeat_fail_count
    global serv_ids
    global shard_map

    # Allow other tasks to run
    await asyncio.sleep(0)

    try:
        # Get the request payload
        payload: dict = await request.get_json()
        ic(payload)

        if payload is None:
            raise Exception('Payload is empty')

        # Get the number of servers to add
        n = int(payload.get('n', -1))

        # Get the list of server replica hostnames to add
        servers: dict[str, list[str]] = dict(payload.get('servers', {}))
        hostnames = list(servers.keys())

        new_shards: list[dict] = list(payload.get('new_shards', []))

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

            # Check if all `hostnames` are in `replicas`
            if not hostnames_set.isdisjoint(replicas_set):
                raise Exception(
                    f'Hostnames `{hostnames_set & replicas_set}` are already in replicas')

            # Check if all shards in `new_shards` are not in `shard_map`
            for shard in new_shards:
                if shard['shard_id'] in shard_map.keys():
                    raise Exception(
                        f'Shard `{shard["shard_id"]}` already exists in shard_map')

            # Check if all shards for all servers in `hostnames` are in `shard_map` union `new_shards`
            for shards in servers.values():
                for shard in shards:
                    if shard not in shard_map.keys() and \
                            shard not in [shard['shard_id'] for shard in new_shards]:
                        raise Exception(
                            f'Shard `{shard}` not found in shard_map or new_shards')
                # END for shard in shards
            # END for shards in servers.values()

            ic(hostnames, new_shards)

            # Spawn new containers
            semaphore = asyncio.Semaphore(DOCKER_TASK_BATCH_SIZE)
            async with Docker() as docker:
                # Define tasks
                tasks = []

                # Add the hostnames to the list
                for hostname in hostnames:
                    replicas.add(hostname)

                    # Edit the flatline map
                    heartbeat_fail_count[hostname] = 0

                    # get new server id
                    serv_id = get_new_server_id()
                    serv_ids[hostname] = serv_id

                    # Add the shards to the shard_locks and shard_map
                    for shard in new_shards:
                        shard_map[shard['shard_id']] = []
                        shard_locks[shard['shard_id']] = FifoLock()
                    # END for shard in new_shards

                    tasks.append(spawn_container(docker, serv_id,
                                                 hostname, semaphore))
                # END for hostname in hostnames

                # Wait for all tasks to complete
                await asyncio.gather(*tasks, return_exceptions=True)

                # Add the shards to the shard_map
                for hostname in hostnames:
                    for shard in servers[hostname]:
                        shard_map[shard].append(hostname)
                    # END for shard in servers[hostname]
                # END for hostname in hostnames
            # END async with Docker

            # Copy shards to the new containers
            semaphore = asyncio.Semaphore(REQUEST_BATCH_SIZE)
            # TODO: Add the shards to the new containers

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
        shards: list[str],
    ):
        # Allow other tasks to run
        await asyncio.sleep(0)

        async with semaphore:
            async with session.post(f'http://{hostname}:5000/config',
                                    json={'shards': shards}) as response:
                await response.read()

            return response
        # END async with semaphore
    # END post_config_wrapper

    async def get_copy_wrapper(
        session: aiohttp.ClientSession,
        hostname: str,
        shards: list[str],
    ):
        # Allow other tasks to run
        await asyncio.sleep(0)

        async with semaphore:
            async with session.get(f'http://{hostname}:5000/copy',
                                   json={'shards': shards}) as response:
                await response.read()

            return response
        # END async with semaphore
    # END get_copy_wrapper

    async def post_write_wrapper(
        session: aiohttp.ClientSession,
        hostname: str,
        payload: Dict[str, Any],
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
        call_server_shards: Dict[str, List[str]] = {}

        # For each shard K in `shards`:
        for shard in shards:
            # Get server A from `shard_map` for the shard K
            server = shard_map[shard][0]  # TODO: Add load balancing

            call_server_shards[server] = call_server_shards.get(server, [])
            call_server_shards[server].append(shard)
        # END for shard in shards

        async with aiohttp.ClientSession() as session:
            # Call /config endpoint on the server S with the hostname
            config_task = asyncio.create_task(
                post_config_wrapper(session, hostname, shards))
            response = await asyncio.gather(*[config_task], return_exceptions=True)
            response = None if isinstance(
                response[0], BaseException) else response[0]

            if response is None or response.status != 200:
                raise Exception(f'Failed to add shards to {hostname}')

            # Call /copy on server A to copy the shard K
            # Define tasks
            tasks = [asyncio.create_task(get_copy_wrapper(
                session, server, shards))
                for server, shards in call_server_shards.items()]

            # Wait for all tasks to complete
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            responses = [None if isinstance(
                response, BaseException) else response for response in responses]

            # Get the data from the responses
            all_data: Dict[str, List[Any]] = {}

            for (response, shards) in zip(responses,
                                          call_server_shards.values()):
                if response is None or response.status != 200:
                    raise Exception(f'Failed to copy shards to {hostname}')

                data: Dict = await response.json()

                for shard in shards:
                    all_data[shard] = data[shard]

            # END for (response, shards) in zip(responses, call_server_shards.values())

            # Call /write on server S to write the shard K
            # Define tasks
            payloads: List[Dict[str, Any]] = []

            pool: asyncpg.Pool = current_app.pool # type: ignore
                
                

    except Exception as e:
        if DEBUG:
            print(f'{Fore.RED}ERROR | '
                  f'{e}'
                  f'{Style.RESET_ALL}',
                  file=sys.stderr)
    # END try-except
# END copy_shards_to_container
