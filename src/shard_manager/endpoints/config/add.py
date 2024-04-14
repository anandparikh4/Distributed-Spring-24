from quart import Blueprint, jsonify, request

from endpoints.config.add_helper import *
from utils import *

from .elect_primary import elect_primary

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
    global shard_primary

    # Allow other tasks to run
    await asyncio.sleep(0)

    try:
        # Convert the reponse to json object
        request_json = await request.get_json()

        if request_json is None:
            raise Exception('Payload is empty')

        # Convert the json response to dictionary
        payload = dict(request_json)
        ic(payload)

        # Get the number of servers to add
        n = int(payload.get('N', -1))

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

        async with common.lock(Write):
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
                    f'Shards `{problems}` are not defined in shard_map or new_shards')

            ic("To add: ", hostnames, new_shards)

            # Add the shards to the shard_locks and shard_map
            for shard in new_shard_ids:
                # Change to ConsistentHashMap
                shard_map[shard] = ConsistentHashMap()

                shard_primary[shard] = ""
            # END for shard in new_shards

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

            elect_primary()
            ic(shard_primary)
            ic(serv_ids)

            await asyncio.sleep(0)

            # Copy shards to the new containers
            semaphore = asyncio.Semaphore(REQUEST_BATCH_SIZE)

            # Define tasks
            tasks = [asyncio.create_task(
                copy_shards_to_container(
                    hostname,
                    servers[hostname],
                    semaphore
                )
            ) for hostname in hostnames]

            # Wait for all tasks to complete
            await asyncio.gather(*tasks, return_exceptions=True)

            # Update the shard_map with the new replicas
            for hostname in hostnames:
                for shard in servers[hostname]:
                    shard_map[shard].add(hostname, serv_ids[hostname])
                # END for shard in servers[hostname]
            # END for hostname in hostnames

            if len(new_shards) > 0:
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
            # END if len(new_shards) > 0

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
