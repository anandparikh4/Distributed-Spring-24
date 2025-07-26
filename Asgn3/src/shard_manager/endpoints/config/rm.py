from quart import Blueprint, jsonify, request

from utils import *

from .elect_primary import elect_primary

blueprint = Blueprint('rm', __name__)


@blueprint.route('/rm', methods=['DELETE'])
async def rm():
    """
    Delete server replica hostname(s) from the list.

    If `len(hostnames) <= n`:
        Delete `hostnames` and `n - len(hostnames)` random hostnames from the list.
        While selecting other random hostnames, only select those servers which are not the only server for any shard.

    If `n <= 0`:
        Return an error message.
    If `n > len(replicas)`:
        Return an error message.
    If `len(hostnames) > n`:
        Return an error message.
    If for any hostname in `hostnames`, `hostname not in replicas`:
        Do not delete any replicas from the list.
        Return an error message.
    If for any hostname S in `hostnames`, there exists some shard K such that S is the only server in `shard_map[K]`:
        Do not delete any replicas from the list.
        Return an error message.

    Random hostnames are deleted from the list of replicas.

    `Request payload:`
        `n: number of servers to delete`
        `hostnames: list of server replica hostnames to delete (<= n) [optional]`

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
    global shard_primary

    # Allow other tasks to run
    await asyncio.sleep(0)

    try:
        # Convert the reponse to json object
        response_json = await request.get_json()

        if response_json is None:
            raise Exception('Payload is empty')

        # Convert the json response to dictionary
        payload = dict(response_json)
        ic(payload)

        # Get the number of servers to delete
        n = int(payload.get('N', -1))

        # Get the list of server replica hostnames to delete
        hostnames: List[str] = list(payload.get('servers', []))

        if n <= 0:
            raise Exception(
                'Number of servers to delete must be greater than 0')

        if n > len(replicas):
            raise Exception(
                'Number of servers to delete must be less than or equal to number of replicas')

        hostnames_set = set(hostnames)

        if len(hostnames_set) != len(hostnames):
            raise Exception('Duplicate hostnames are not allowed in the list')

        if len(hostnames) != n:
            raise Exception(
                'Length of hostname list is not equal to no. of instances to delete')

        async with common.lock(Write):

            for shard, servers in shard_map.items():
                remaining = set(servers.getServerList()) - hostnames_set
                if len(remaining) == 0:
                    raise Exception(f'Cannot delete all servers. '
                                    f'Shard `{shard}` will be left without any server.')

            ic("To delete: ", hostnames)

            for shard, primary in shard_primary.items():
                if primary in hostnames_set:
                    shard_primary[shard] = ""

            semaphore = asyncio.Semaphore(DOCKER_TASK_BATCH_SIZE)

            async def remove_container(
                docker: Docker,
                hostname: str
            ):
                # Allow other tasks to run
                await asyncio.sleep(0)

                async with semaphore:
                    try:
                        # stop docker containers for the deleted hostnames
                        container = await docker.containers.get(hostname)

                        await container.kill(signal='SIGTERM')
                        await container.wait()
                        await container.delete(force=True)

                        if DEBUG:
                            print(f'{Fore.LIGHTYELLOW_EX}REMOVE | '
                                  f'Deleted container for {hostname}'
                                  f'{Style.RESET_ALL}',
                                  file=sys.stderr)

                    except Exception as e:
                        if DEBUG:
                            print(f'{Fore.RED}ERROR | '
                                  f'{e.__class__.__name__}: {e}'
                                  f'{Style.RESET_ALL}',
                                  file=sys.stderr)
                    # END try-except
                # END async with semaphore
            # END stop_container

            async with Docker() as docker:
                # Define tasks
                tasks = []

                # Delete the hostnames from the list
                for hostname in hostnames:
                    # Remove the server from the list
                    replicas.remove(hostname)

                    # Edit the flatline map
                    heartbeat_fail_count.pop(hostname, None)

                    # Remove the server id
                    serv_ids.pop(hostname, None)

                    # Remove server from shard_map
                    for servers in shard_map.values():
                        _servers = servers.getServerList().copy()
                        if hostname in _servers:
                            servers.remove(hostname)

                    tasks.append(
                        asyncio.create_task(
                            remove_container(
                                docker,
                                hostname
                            )
                        )
                    )
                # END for

                # Wait for all tasks to complete
                await asyncio.gather(*tasks, return_exceptions=True)
            # END async with Docker

            final_hostnames = ic(replicas.getServerList())
        # END async with common.lock(Write)

        elect_primary()
        ic(shard_primary)

        # Return the response payload
        return jsonify(ic({
            'message': {
                'N': len(replicas),
                'replicas': final_hostnames
            },
            'status': 'success'
        })), 200

        # END async with lock

    except Exception as e:

        return jsonify(ic(err_payload(e))), 400
    # END try-except
# END rm
