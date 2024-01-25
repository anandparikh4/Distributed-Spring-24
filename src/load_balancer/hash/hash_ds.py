from typing import Callable

# consistent hashing data structure


class ConsistentHashMap:

    # constructor
    def __init__(
        self,
        request_hash: Callable[[int], int],
        server_hash: Callable[[int, int], int],
        hostnames=None,
        n_slots: int = 512,
        n_virtual: int = 9,
        probing: str = 'linear'
    ):

        # assign the hash functions
        self.requestHash = request_hash
        self.serverHash = server_hash

        # map: server-name -> server-index
        self.servers: dict[str, int] = {}

        # slots
        self.slots: list[None | str] = [None] * n_slots
        self.n_slots = n_slots

        self.probing = probing.lower()

        # number of virtual copies to keep
        self.n_virtual = n_virtual
        if hostnames is None:
            # default hostnames
            hostnames = ["Server-1", "Server-2", "Server-3"]

        for hostname in hostnames:
            # populate slots
            self.add(hostname)

    # length
    def __len__(self):
        return len(self.servers.keys())

    # probing function
    def probe(self, hashval: int, i: int) -> int:
        if self.probing == 'quadratic':
            return hashval + i*i

        return hashval + i

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
            server_hash = (self.serverHash(
                server_idx + 1, virtual_idx + 1)) % self.n_slots
            # Probe if there is collision
            # while self.slots[server_hash] is not None:
            #     server_hash = (server_hash + 1) % self.n_slots
            i = 1
            idx = server_hash
            while self.slots[idx] is not None:
                idx = self.probe(server_hash, i) % self.n_slots
                i += 1
            self.slots[idx] = hostname

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
            server_hash = (self.serverHash(
                server_idx + 1, virtual_idx + 1)) % self.n_slots
            # Probing if there is collision
            # while self.slots[server_hash] != hostname:
            #     server_hash = (server_hash + 1) % self.n_slots
            i = 1
            idx = server_hash
            while self.slots[idx] != hostname:
                idx = self.probe(server_hash, i) % self.n_slots
                i += 1
            self.slots[idx] = None

    # find the server (by hostname) to which to route the request
    def find(self, request_id: int):
        '''
            If no server present, cannot map request: raise error
            Else, return the cyclically next server's hostname
        '''
        if len(self.servers) == 0:
            raise KeyError("No servers alive")
        request_hash = (self.requestHash(request_id)) % self.n_slots
        # Here linear probing is required since nearest server is required
        while self.slots[request_hash] is None:
            request_hash = (request_hash + 1) % self.n_slots
        return self.slots[request_hash]

    # get list of all hostnames of servers
    def getServerList(self):
        return list(self.servers.keys())

    # get remaining servers, i.e. maximum number of servers that can be added
    def remaining(self):
        return self.slots.count(None) // self.n_virtual
