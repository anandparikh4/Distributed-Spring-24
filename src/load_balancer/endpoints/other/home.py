from quart import Blueprint, jsonify

from utils import *

blueprint = Blueprint('home', __name__)


@blueprint.route('/home', methods=['GET'])
async def home():
    """
    Load balance the request to the server replicas.

    `Response payload:`
        `message: message from server`
        `status: status of the request`

    `Error payload:`
        `message: error message`
        `status: status of the request`
    """

    global replicas

    # To allow other tasks to run
    await asyncio.sleep(0)

    try:
        # Generate a random request id
        request_id = get_request_id()
        ic(request_id)

        async with lock(Read):
            server_name = replicas.find(request_id)

        if server_name is None:
            raise Exception('No servers are available')

        ic(server_name)

        async def get_home_wrapper(
            session: aiohttp.ClientSession,
            server_name: str
        ):
            # To allow other tasks to run
            await asyncio.sleep(0)

            async with session.get(f'http://{server_name}:5000/home') as response:
                await response.read()

            return response
        # END get_home_wrapper

        # Convert to aiohttp request
        timeout = aiohttp.ClientTimeout(connect=REQUEST_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            task = asyncio.create_task(
                get_home_wrapper(
                    session,
                    server_name
                )
            )

            serv_response = await asyncio.gather(*[task], return_exceptions=True)
            serv_response = serv_response[0] if not isinstance(
                serv_response[0], BaseException) else None
        # END async with

        if serv_response is None:
            raise Exception('Server did not respond')

        return jsonify(ic(await serv_response.json())), 200

    except Exception as e:
        if DEBUG:
            print(f'{Fore.RED}ERROR | '
                  f'{e}'
                  f'{Style.RESET_ALL}',
                  file=sys.stderr)

        return jsonify(ic(err_payload(e))), 400
    # END try-except
# END home
