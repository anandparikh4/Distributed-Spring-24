from quart import Blueprint, jsonify, request

from utils import *

from .add import spawn_container

blueprint = Blueprint('init', __name__)


@blueprint.route('/init', methods=['POST'])
async def init():
    """
    Initiaize the server replicas and shards.

    If `len(servers) == n`:
        Add `servers` to the list.
    If `shards` is not empty:
        Add the new shards to the `shard_map`.

    If `len(servers) != n`:
        Return an error message.
    If relevant fields are not present in `shards`:
        Return an error message.
    If `n > remaining slots`:
        Return an error message.
    If some shard for some server in `servers` does not exist in `shards`:
        Return an error message.

    `Request payload:`
        `n: number of servers to add`
        `shards: list of new shard names and description to add [optional]`
            `stud_id_low: lower bound of student id`
            `shard_id: name of the shard`
            `shard_size: size of the shard`
        `servers: dict of server hostname -> list of shard names to add [new shard names must be define in `shards`]`

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

    # Allow other tasks to run
    await asyncio.sleep(0)

    async def post_config_wrapper(
        semaphore: asyncio.Semaphore,
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

    try:
        # Convert the reponse to json object
        response_json = await request.get_json()

        if response_json is None:
            raise Exception('Payload is empty')

        # Convert the json response to dictionary
        payload = dict(response_json)
        ic(payload)

        # Get the number of servers to add
        n = int(payload.get('N', -1))

        # Get the list of server replica hostnames to add
        servers: Dict[str, List[str]] = dict(payload.get('servers', {}))
        hostnames = list(servers.keys())

        new_shards: List[Dict[str, Any]] = list(payload.get('shards', []))

        if len(hostnames) != n:
            raise Exception(
                'Length of server list is not equal to instances to add')

        # Check if relevant fields are present
        for shard in new_shards:
            if not all(k in shard.keys()
                       for k in
                       ['stud_id_low', 'shard_id', 'shard_size']):
                raise Exception('Invalid shard description')
        # END for shard in new_shards

        async with common.lock(Write):
            # Check is slots are available
            if n > replicas.remaining():
                raise Exception(
                    f'Insufficient slots. Only {replicas.remaining()} slots left')

            # Check if all shards for all servers in `hostnames` are in `shard_map` union `new_shards`
            new_shard_ids: Set[str] = set(shard['shard_id']
                                          for shard in new_shards)

            problems = set()
            for shards in servers.values():
                problems |= set(shards) - new_shard_ids

            if len(problems) > 0:
                raise Exception(
                    f'Shards `{problems}` are not defined in new_shards')

            ic("To add: ", hostnames, new_shards)

            # Add the shards to the shard_locks and shard_map
            for shard in new_shard_ids:
                # Change to ConsistentHashMap
                shard_map[shard] = ConsistentHashMap()
            # END for shard in new_shards

            # Spawn new containers
            docker_semaphore = asyncio.Semaphore(DOCKER_TASK_BATCH_SIZE)

            async with Docker() as docker:
                # Define tasks
                tasks = []

                # Add the hostnames to the list
                for hostname in hostnames:
                    # get new server id
                    serv_id = get_new_server_id()
                    serv_ids[hostname] = serv_id

                    # Add the hostname to the replicas list
                    replicas.add(hostname, serv_id)

                    # Edit the flatline map
                    heartbeat_fail_count[hostname] = 0

                    # Update the shard_map with the new replicas
                    for shard in servers[hostname]:
                        shard_map[shard].add(hostname, serv_id)
                    # END for shard in servers[hostname]

                    tasks.append(
                        asyncio.create_task(
                            spawn_container(
                                docker=docker,
                                serv_id=serv_id,
                                hostname=hostname,
                                semaphore=docker_semaphore
                            )
                        )
                    )
                # END for hostname in hostnames

                # Wait for all tasks to complete
                res = await asyncio.gather(*tasks, return_exceptions=True)

                if any(res):
                    raise Exception('Failed to spawn containers')
            # END async with Docker

            ic(serv_ids)

            await asyncio.sleep(0)

            # config the new shards
            req_semaphore = asyncio.Semaphore(REQUEST_BATCH_SIZE)
            timeout = aiohttp.ClientTimeout(connect=REQUEST_TIMEOUT)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                # Define tasks
                tasks = [asyncio.create_task(
                    post_config_wrapper(
                        semaphore=req_semaphore,
                        session=session,
                        hostname=hostname,
                        payload={
                            "shards": servers[hostname]
                        }
                    )
                ) for hostname in hostnames]

                # Wait for all tasks to complete
                config_responses = await asyncio.gather(*tasks, return_exceptions=True)
                config_responses = [None if isinstance(response, BaseException)
                                    else response
                                    for response in config_responses]

                for (hostname, response) in zip(hostnames, config_responses):
                    if response is None or response.status != 200:
                        raise Exception(f'Failed to add shards to {hostname}')

            async with common.pool.acquire() as conn:
                async with conn.transaction():
                    stmt = await conn.prepare(
                        '''--sql
                        INSERT INTO shardT (
                            stud_id_low,
                            shard_id,
                            shard_size)
                        VALUES (
                            $1::INTEGER,
                            $2::TEXT,
                            $3::INTEGER);
                        ''')

                    await stmt.executemany(
                        [(shard['stud_id_low'],
                          shard['shard_id'],
                          shard['shard_size'])
                         for shard in new_shards])
                # END async with conn.transaction()
            # END async with common.pool.acquire() as conn

            final_hostnames = ic(replicas.getServerList())
        # END async with common.lock(Write)

        # Return the response payload
        return jsonify(ic({
            'message': {
                'N': len(replicas),
                'replicas': final_hostnames,
            },
            'status': 'success'
        })), 200

    except Exception as e:

        return jsonify(ic(err_payload(e))), 400
    # END try-except
# END add
