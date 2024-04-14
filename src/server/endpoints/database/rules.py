import common
from common import *


# Bookkeeping for enforcing synchronization between shard replicas across servers
# Simply do 'await bookkeeping(shard_id, term, op)' in each op. No return value
async def bookkeeping(
    shard_id: str,
    term: int,
    op: str
):
    """
        Rule:   If ((term > last_idx) || (term == last_idx && op == "r")){
                    If(!executed) execute
                }
                Else If ((term < last_idx) || (term == last_idx && op != "r")){
                    rollback
                }
    """

    try:
        async with common.pool.acquire() as conn:
            async with conn.transaction():  # perform bookkeeping by executing database operations

                # get the term from TermT
                term_row = await conn.fetchrow('''--sql
                    SELECT *
                    FROM TermT
                    WHERE shard_id = $1::TEXT;
                ''',shard_id)

                if term_row is None:
                    raise Exception(f"Failed to performing bookkeeping")
                last_idx = term_row["last_idx"]
                executed = term_row["executed"]

                # last write/update/delete was successful was not executed on database, so execute now
                if(((term > last_idx) or (term == last_idx and op == "r")) and executed == False):

                    # update TermT executed to True
                    await conn.execute('''--sql
                        UPDATE TermT
                        SET executed = TRUE
                        WHERE shard_id = $1::TEXT                   
                    ''',shard_id)

                    # get the log from LogT                        
                    log_row = await conn.fetchrow('''--sql
                        SELECT *
                        FROM LogT
                        WHERE shard_id = $1::TEXT
                        AND log_idx = $2::INTEGER
                    ''',shard_id,last_idx)

                    if log_row is None:
                        raise Exception(f"Failed to perform bookkeeping")
                    operation = log_row["operation"]
                    stud_id = log_row["stud_id"]
                    content = dict(log_row["content"])
                    
                    # do different things for different operations
                    # write
                    if(operation == "w"):
                        stmt = await conn.prepare('''--sql
                            INSERT INTO StudT (stud_id , stud_name , stud_marks , shard_id)
                            VALUES ($1::INTEGER,
                                    $2::TEXT,
                                    $3::INTEGER,
                                    $4::TEXT);
                        ''')
                        await stmt.executemany([(
                            int(key),
                            str(content[key][0]),
                            int(content[key][1]),
                            shard_id
                        ) for key in content])
                    # update
                    elif(operation == "u"):
                        await conn.execute('''--sql
                            UPDATE StudT
                            SET stud_name = $3::TEXT , stud_marks = $4::INTEGER
                            WHERE shard_id = $1::TEXT
                            AND stud_id = $2::INTEGER
                        ''',shard_id,stud_id,str(content[stud_id][0]),int(content[stud_id][1]))
                    # delete
                    elif(operation == "d"):
                        await conn.execute('''--sql
                            DELETE FROM StudT
                            WHERE stud_id = $1
                        ''',stud_id)
                    # read
                    else:   # operation == "r"
                        pass

                # last write/update/delete was not successful, so rollback
                elif((term < last_idx) or (term == last_idx and op != "r")):
                    
                    # update TermT last_idx to last_idx-1 and executed to True
                    await conn.execute('''--sql
                        UPDATE TermT
                        SET last_idx = $2::INTEGER , executed = TRUE
                        WHERE shard_id = $1::TEXT
                    ''',shard_id,last_idx-1)

                    # delete last_idx log from LogT
                    await conn.execute('''--sql
                        DELETE FROM LogT
                        WHERE shard_id = $1::TEXT
                        AND log_idx = $2::INTEGER
                    ''',shard_id,last_idx)

    except Exception as e:
        raise e
