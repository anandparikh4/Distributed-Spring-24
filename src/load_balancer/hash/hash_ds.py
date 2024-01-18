# tested some cryptographic hash algorithms, did not make any discernible improvements
# import hashlib

# consistent hashing data structure
class ConsistentHashMap:

    # constructor
    def __init__(self, hostnames=None, n_slots=512, n_virtual=9):
        self.servers: dict[str, int] = {}                       # map: server-name -> server-index
        self.slots: list[None | str] = [None] * n_slots         # slots
        self.n_slots = n_slots
        self.n_virtual = n_virtual                              # number of virtual copies to keep
        if hostnames is None:
            hostnames = ["Server-1", "Server-2", "Server-3"]    # default hostnames
        for hostname in hostnames:
            self.add(hostname)                                  # populate slots

    # length
    def __len__(self):
        return len(self.servers)

    '''
        Note about hash functions: 
        The coefficients are - 
            1) Large : To cover the whole slot-space nearly uniformly with larger distance b/w virtual copies
            2) Prime : So finally taking modulo by any slot size does not reduce randomness
    '''

    # hash function for request
    def requestHash(self, i: int):
        hash_int = 1427*i*i + 2503*i + 2003
        # hash_bytes = hashlib.sha256(hash_int.to_bytes(4 , 'big')).digest()
        # hash_int = int.from_bytes(hash_bytes , 'big')
        return hash_int

    # hash function for server
    def serverHash(self, i: int, j: int):
        hash_int = 1249*i*i + 2287*j*j + 1663*j + 2287
        # hash_bytes = hashlib.sha256(hash_int.to_bytes(4 , 'big')).digest()
        # hash_int = int.from_bytes(hash_bytes , 'big')
        return hash_int

    # add a server (by hostname)
    def add(self, hostname: str):
        '''
            If empty slots < n_virtual, cannot add new server: raise error
            Else If server's hostname is present, cannot duplicate: raise error
            Else add all virtual copies of the server to the slots
        '''
        if self.slots.count(None) < self.n_virtual:
            raise IndexError("Insufficient slots to add new server")
        if hostname in self.servers.keys():
            raise KeyError("Hostname already present")
        server_idx = 0
        for value in sorted(list(self.servers.values())):
            if server_idx != value:
                break
            server_idx += 1
        self.servers[hostname] = server_idx
        for virtual_idx in range(self.n_virtual):
            server_hash = (self.serverHash(server_idx + 1, virtual_idx + 1)) % self.n_slots
            while self.slots[server_hash] is not None:
                server_hash = (server_hash + 1) % self.n_slots
            self.slots[server_hash] = hostname

    # remove a server (by hostname)
    def remove(self, hostname: str):
        '''
            If server's hostname is not found, cannot remove: raise error
            Else remove all virtual copies of the server from the slots
        '''
        if hostname not in self.servers.keys():
            raise KeyError("Hostname not found")
        server_idx = self.servers[hostname]
        self.servers.pop(hostname)
        for virtual_idx in range(self.n_virtual):
            server_hash = (self.serverHash(server_idx + 1, virtual_idx + 1)) % self.n_slots
            while self.slots[server_hash] != hostname:
                server_hash = (server_hash + 1) % self.n_slots
            self.slots[server_hash] = None

    # find the server (by hostname) to which to route the request
    def find(self, request_id: int):
        '''
            If no server present, cannot map request: raise error
            Else, return the cyclically next server's hostname
        '''
        if len(self.servers) == 0:
            raise KeyError("No servers alive")
        request_hash = (self.requestHash(request_id)) % self.n_slots
        while self.slots[request_hash] is None:
            request_hash = (request_hash + 1) % self.n_slots
        return self.slots[request_hash]

    # get list of all hostnames of servers
    def getServerList(self):
        return list(self.servers.keys())

    # get remaining servers, i.e. maximum number of servers that can be added
    def remaining(self):
        return self.slots.count(None) // self.n_virtual
