import common
from common import *


# All rules for enforcing synchronization between shard replicas across servers
# Simply do 'await rules(shard_id, term)' in each operation. No return value
async def rules(
    shard_id: str,
    valid_at: int
):
    """
        Rule 1 : Erase all entries where created_at > valid_idx or (deleted_at is not null and deleted_at <= valid_idx)
        Rule 2 : Update deleted_at = null all entries where deleted_at > valid_idx
    """

    try:
        async with common.pool.acquire() as conn:
            async with conn.transaction():
                # Enforce rules by executing database operations
                await conn.execute('''--sql
                    DELETE FROM StudT
                    WHERE (shard_id = $1::TEXT)
                        AND (created_at > $2::INTEGER
                            OR (deleted_at IS NOT NULL
                                AND deleted_at <= $2::INTEGER));
                    ''', shard_id, valid_at)

                await conn.execute('''--sql
                    UPDATE StudT
                    SET deleted_at = NULL
                    WHERE shard_id = $1::TEXT
                        AND deleted_at > $2::INTEGER;
                    ''', shard_id, valid_at)

    except Exception as e:

        raise e
