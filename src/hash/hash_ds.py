# consistent hashing data structure
class ConsistentHashMap:

    # constructor
    def __init__(self, hostnames=None, n_slots=512, n_virtual=9):
        self.servers = {}
        self.slots = [None] * n_slots
        self.n_slots = n_slots
        self.n_virtual = n_virtual
        if hostnames is None:
            hostnames = ["Server 1", "Server 2", "Server 3"]
        for hostname in hostnames:
            self.add(hostname)

    # hash function for request
    @staticmethod
    def requestHash(i):
        return i*i + 2*i + 17

    # hash function for server
    @staticmethod
    def serverHash(i, j):
        return i*i + j*j + 2*j + 25

    # add a server (by hostname)
    def add(self, hostname):
        if self.slots.count(None) < self.n_virtual:
            raise IndexError("Insufficient slots to add new server")
        if hostname in self.servers.keys:
            raise KeyError("Hostname already present")
        server_idx = 0
        for value in sorted(list(self.servers.values())):
            server_idx += 1
            if server_idx != value:
                break
        self.servers[hostname] = server_idx
        for virtual_idx in range(self.n_virtual):
            server_hash = (self.serverHash(server_idx+1, virtual_idx+1)) % self.n_slots
            while self.slots[server_hash] is not None:
                server_hash = (server_hash+1) % self.n_slots
            self.slots[server_hash] = hostname

    # remove a server (by hostname)
    def remove(self, hostname):
        if hostname not in self.servers.keys:
            raise KeyError("Hostname not found")
        server_idx = self.servers[hostname]
        self.servers.pop(hostname)
        for virtual_idx in range(self.n_virtual):
            server_hash = (self.serverHash(server_idx+1, virtual_idx+1)) % self.n_slots
            while self.slots[server_hash] != hostname:
                server_hash = (server_hash+1) % self.n_slots
            self.slots[server_hash] = None

    # find the server (by hostname) to which to route the request
    def find(self, request_id):
        request_hash = (self.requestHash(request_id)) % self.n_slots
        while self.slots[request_hash] is None:
            request_hash = (request_hash+1) % self.n_slots
        return self.slots[request_hash]

    # get list of all hostnames of servers
    def getServerList(self):
        return list(self.servers.keys())
