from utils import *


def elect_primary(blacklist: Optional[List[str]] = None):
    """
    Scan the primary ds, and elect a new primary if the current primary is empty.
    """

    global shard_primary

    if blacklist is None:
        blacklist = []

    for shard in shard_primary:
        if shard_primary[shard] == "" or shard_primary[shard] in blacklist:
            servers = shard_map[shard].getServerList()
            shard_primary[shard] = helper(servers, blacklist)
        # END if primary[shard] == "" or primary[shard] in blacklist
    # END for shard in primary
# END elect_primary


def helper(
    servers: List[str],
    blacklist: List[str]
) -> str:
    """
    Select a random server from the list of servers that is not in the blacklist.
    """

    if len(servers) == 0:
        return ""

    # Remove the servers in the blacklist
    servers = [server
               for server in servers
               if server not in blacklist]

    if len(servers) == 0:
        return ""

    return random.choice(servers)
# END helper
