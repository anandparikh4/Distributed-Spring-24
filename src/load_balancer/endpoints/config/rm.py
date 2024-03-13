from quart import Blueprint, current_app, jsonify, request

from utils import *

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

    # Allow other tasks to run
    await asyncio.sleep(0)

    try:
        # Get the request payload
        payload = dict(await request.get_json())
        ic(payload)

        if payload is None:
            raise Exception('Payload is empty')

        # Get the number of servers to delete
        n = int(payload.get('n', -1))

        # Get the list of server replica hostnames to delete
        hostnames: List[str] = list(payload.get('hostnames', []))

        if n <= 0:
            raise Exception(
                'Number of servers to delete must be greater than 0')

        if n > len(replicas):
            raise Exception(
                'Number of servers to delete must be less than or equal to number of replicas')

        if len(hostnames) > n:
            raise Exception(
                'Length of hostname list is more than instances to delete')

        singles = {servers.getServerList()[0]: shard
                   for shard, servers in shard_map.items()
                   if len(servers) == 1}

        single_problems = {server: shard
                           for server, shard in singles.items()
                           if server in hostnames}

        if len(single_problems) > 0:
            raise Exception(
                f'Only one copy of shards `{single_problems.values()}` is '
                f'present in the hostnames `{single_problems.keys()}` '
                f'to delete, respectively.')

        async with lock(Write):
            choices = set(replicas.getServerList())

            # Convert hostnames to set for faster lookup
            hostnames_set = set(hostnames)

            # Check if all `hostnames` are in `replicas`
            if not hostnames_set.issubset(choices):
                raise Exception(
                    f'Hostnames `{hostnames_set - choices}` are not in replicas')

            # remove `hostnames` from `choices`
            choices = list(choices - hostnames_set - singles.keys())

            if (_k := len(choices) + len(hostnames)) < n:
                raise Exception(
                    f'Not enough replicas to delete. '
                    f'Only {_k} replicas can be deleted.')

            # Choose `n - len(hostnames)` random hostnames from the list without replacement
            random_hostnames = random.sample(choices, k=n - len(hostnames))

            # Add the random hostnames to the list of hostnames to delete
            hostnames.extend(random_hostnames)

            ic("To delete: ", hostnames)

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

                        await container.stop(timeout=STOP_TIMEOUT)
                        await container.delete(force=True)

                        if DEBUG:
                            print(f'{Fore.LIGHTYELLOW_EX}REMOVE | '
                                  f'Deleted container for {hostname}'
                                  f'{Style.RESET_ALL}',
                                  file=sys.stderr)

                    except Exception as e:
                        if DEBUG:
                            print(f'{Fore.RED}ERROR | '
                                  f'{e}'
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
        # END async with lock(Write)

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
        if DEBUG:
            print(f'{Fore.RED}ERROR | '
                  f'{e}'
                  f'{Style.RESET_ALL}',
                  file=sys.stderr)

        return jsonify(ic(err_payload(e))), 400
    # END try-except
# END rm
