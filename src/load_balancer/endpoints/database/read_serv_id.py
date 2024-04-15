from quart import Blueprint, Response, jsonify

from utils import *

blueprint = Blueprint('read_serv_id', __name__)


@blueprint.route('/read/<int:server_id>', methods=['GET'])
async def read(server_id: int):
    """
    Response Payload:
        `sh1`: [data],
        `sh2`: [data],
        ...
    """

    await asyncio.sleep(0)

    async def get_server_from_id_wrapper(
        session: aiohttp.ClientSession,
        server_id: int
    ):

        # To allow other tasks to run
        await asyncio.sleep(0)

        async with session.get(f'http://Shard-Manager:5000/get_server_from_id/{server_id}') as response:
            await response.read()

        return response
    # END get_server_from_id_wrapper

    async def copy_get_wrapper(
        session: aiohttp.ClientSession,
        server_name: str,
        json_payload: Dict
    ):

        # To allow other tasks to run
        await asyncio.sleep(0)

        async with session.get(f'http://{server_name}:5000/copy',
                               json=json_payload) as response:
            await response.read()

        return response
    # END read_get_wrapper

    try:
        timeout = aiohttp.ClientTimeout(connect=REQUEST_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            task = [asyncio.create_task(
                get_server_from_id_wrapper(
                    session,
                    server_id
                )
            )]

            response = await asyncio.gather(*task, return_exceptions=True)
            response = [None if isinstance(r, BaseException)
                        else r for r in response]
            response = response[0]

            if response is None or response.status != 200:
                raise Exception('Server not found')

            server_info = dict(await response.json())
            server_name = str(server_info['server'])
            server_shards: List[str] = list(server_info['shards'])

            shard_ids: List[str] = []
            shard_valid_ats: List[int] = []

            async with common.pool.acquire() as conn:
                async with conn.transaction():
                    async for record in conn.cursor(
                        '''--sql
                    SELECT
                        shard_id,
                        valid_at
                    FROM
                        ShardT
                    WHERE
                        shard_id = ANY($1::TEXT[])
                    FOR SHARE;
                    ''', server_shards):

                        shard_ids.append(record["shard_id"])
                        shard_valid_ats.append(record["valid_at"])
                    # END async for record in conn.cursor

                    task = [asyncio.create_task(
                        copy_get_wrapper(
                            session=session,
                            server_name=server_name,
                            json_payload={
                                "shards": shard_ids,
                                "terms": shard_valid_ats,
                            }
                        )
                    )]

                    response = await asyncio.gather(*task, return_exceptions=True)
                    response = [None if isinstance(r, BaseException)
                                else r for r in response]
                    response = response[0]

                    if response is None or response.status != 200:
                        raise Exception("Can't read data from server")

                    data = dict(await response.json())

                    return jsonify(data['data']), 200

    except Exception as e:
        return jsonify(ic(err_payload(e))), 400

# END read_serv_id
