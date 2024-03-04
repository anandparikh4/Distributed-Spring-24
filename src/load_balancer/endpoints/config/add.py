from quart import Blueprint, current_app, jsonify, request

from common import *

blueprint = Blueprint('add', __name__)


@blueprint.route('/add', methods=['POST'])
async def add():
    """
    Add new server replica hostname(s) to the list.

    If `len(hostnames) <= n`:
        Add `hostnames` and `n - len(hostnames)` random hostnames to the list.
    If `n <= 0`:
        Return an error message.
    If `len(hostnames) > n`:
        Return an error message.
    If `hostnames` contains duplicates:
        Return an error message.
    If `n > remaining slots`:
        Return an error message.
    If `hostname` in `hostnames` already exists in `replicas`:
        Do not add any replicas to the list.
        Return an error message.

    Random hostnames are generated using the `random_hostname()` function.

    `Request payload:`
        `n: number of servers to add`
        `hostnames: list of server replica hostnames to add (<= n) [optional]`

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
    global serv_id

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
        hostnames: list[str] = list(payload.get('hostnames', []))

        if n <= 0:
            raise Exception(
                'Number of servers to add must be greater than 0')

        if len(hostnames) > n:
            raise Exception(
                'Length of hostname list is more than instances to add')

        if len(hostnames) != len(set(hostnames)):
            raise Exception('Hostname list contains duplicates')

        # Generate `n - len(hostnames)` random hostnames
        new_hostnames: set[str] = set()
        while len(new_hostnames) < n - len(hostnames):
            new_hostnames.add(random_hostname())

        # Add `new_hostnames` to the list.
        hostnames.extend(new_hostnames)

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

            ic("To add: ", hostnames)

            semaphore = asyncio.Semaphore(DOCKER_TASK_BATCH_SIZE)

            async def spawn_container(
                docker: Docker,
                serv_id: int,
                hostname: str
            ):
                # Allow other tasks to run
                await asyncio.sleep(0)

                async with semaphore:
                    try:
                        # spawn new docker containers for the new hostnames
                        container_config = {
                            'image': 'server:v1',
                            'detach': True,
                            'env': [f'SERVER_ID={serv_id}',
                                    'DEBUG=true'],
                            'hostname': hostname,
                            'tty': True,
                        }

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

                    except Exception as e:
                        if DEBUG:
                            print(f'{Fore.RED}ERROR | '
                                  f'{e}'
                                  f'{Style.RESET_ALL}',
                                  file=sys.stderr)
                    # END try-except
                # END async with semaphore
            # END spawn_container

            async with Docker() as docker:
                # Define tasks
                tasks = []

                # Add the hostnames to the list
                for hostname in hostnames:
                    replicas.add(hostname)

                    # Edit the flatline map
                    heartbeat_fail_count[hostname] = 0

                    # increment server id
                    serv_id += 1

                    tasks.append(spawn_container(docker, serv_id, hostname))
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

    except Exception as e:
        if DEBUG:
            print(f'{Fore.RED}ERROR | '
                  f'{e}'
                  f'{Style.RESET_ALL}',
                  file=sys.stderr)

        return jsonify(ic(err_payload(e))), 400
    # END try-except
# END add
