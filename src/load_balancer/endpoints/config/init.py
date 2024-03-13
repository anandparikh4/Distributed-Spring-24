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

        async with lock(Write):
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

            # Spawn new containers
            semaphore = asyncio.Semaphore(DOCKER_TASK_BATCH_SIZE)

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

                    # Add the shards to the shard_locks and shard_map
                    for shard in new_shard_ids:
                        # Change to ConsistentHashMap
                        shard_map[shard] = ConsistentHashMap()
                        shard_locks[shard] = FifoLock()
                    # END for shard in new_shards

                    # Update the shard_map with the new replicas
                    for shard in servers[hostname]:
                        shard_map[shard].add(hostname, serv_id)
                    # END for shard in servers[hostname]

                    tasks.append(
                        asyncio.create_task(
                            spawn_container(
                                docker,
                                serv_id,
                                hostname,
                                semaphore
                            )
                        )
                    )
                # END for hostname in hostnames

                # Wait for all tasks to complete
                await asyncio.gather(*tasks, return_exceptions=True)
            # END async with Docker

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

                async with conn.transaction(isolation='serializable'):
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
