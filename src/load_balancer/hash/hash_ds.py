from typing import Callable
import bisect

from consts import HASH_NUM
from .hash_functions import requestHashList, serverHashList

# consistent hashing data structure

class ConsistentHashMap:

    # constructor
    def __init__(
        self,
        request_hash: Callable[[int], int] = requestHashList[HASH_NUM],
        server_hash: Callable[[int, int], int] = serverHashList[HASH_NUM],
        n_slots: int = 512,
        n_virtual: int = 9,
        probing: str = 'quadratic'
    ):

        # assign the hash functions
        self.requestHash = request_hash
        self.serverHash = server_hash

        # map: server-name -> server-id
        self.servers: dict[str, int] = {}
        # map: server-name -> slots of virtual replicas
        self.replicas: dict[str , list[int]] = {}

        # id of next server
        self.next_server: list[None | str] = [None] * n_slots
        self.n_slots = n_slots

        # slot numbers occupied by servers
        self.server_slots = []

        self.probing = probing.lower()

        # number of virtual copies to keep
        self.n_virtual = n_virtual

    # length
    def __len__(self):
        return len(self.servers)

    # probing function
    def probe(self, hashval: int, i: int):
        if self.probing == 'quadratic':
            return hashval + i * i

        return hashval + i

    # add a server (by hostname and hostid)
    # Time Complexity : O(n_slots * n_virtual)
    def add(self, hostname: str, hostid: int):
        '''
            If empty slots < n_virtual, cannot add new server: raise error
            Else If server's hostname or hostid is present, cannot duplicate: raise error
            Else add all virtual copies of the server to the slots
        '''
        if self.n_slots - len(self.server_slots) < self.n_virtual:
            raise RuntimeError("Insufficient slots to add new server")
        if hostname in self.servers.keys():
            raise KeyError("Hostname already present")
        if hostid in self.servers.values():
            raise KeyError("Hostid already present")

        self.servers[hostname] = hostid
        self.replicas[hostname] = []

        for virtual_idx in range(self.n_virtual):
            server_hash = (self.serverHash(
                hostid, virtual_idx + 1)) % self.n_slots
            # Probe if there is collision
            i = 0
            slot = server_hash
            while slot in self.server_slots:
                i += 1
                slot = self.probe(server_hash, i) % self.n_slots
            # insert in sorted ordered server_slots and list of virtual slots
            bisect.insort(self.server_slots, slot)
            self.replicas[hostname].add(slot)
            i = 0
            while self.server_slots[i] != slot:
                i += 1
            i -= 1
            if i == -1:
                i = len(self.server_slots) - 1
            i = (self.server_slots[i]+1) % self.n_slots
            while i != slot:
                self.next_server[i] = hostname
                i = (i+1) % self.n_slots
            self.next_server[slot] = hostname

    # remove a server (by hostname)
    # Time Complexity : O(n_slots * n_virtual)
    def remove(self, hostname: str):
        '''
            If server's hostname is not found, cannot remove: raise error
            Else remove all virtual copies of the server from the slots
        '''
        if hostname not in self.servers.keys():
            raise KeyError("Hostname not found")
        hostid = self.servers[hostname]
        self.servers.pop(hostname)

        if (len(self.servers) == 0):
            self.next_server = [None] * self.n_slots
            self.server_slots = []
            self.replicas.pop(hostname)
            return

        for slot in self.replicas[hostname]:
            i = 0
            while self.server_slots[i] != slot:
                i += 1
            i -= 1
            if i == -1:
                i = len(self.server_slots) - 1
            i = self.server_slots[i]
            other_hostname = self.next_server[i]
            i = (i+1) % self.n_slots
            while i != slot:
                self.next_server[i] = other_hostname
                i = (i+1) % self.n_slots
            self.next_server[slot] = other_hostname
            self.server_slots.remove(slot)

        self.replicas.pop(hostname)

    # find the server (by hostname) to which to route the request
    # Time Complexity : O(1)
    def find(self, request_id: int):
        '''
            If no server present, cannot map request: raise error
            Else, return the cyclically next server's hostname
        '''
        request_hash = (self.requestHash(request_id)) % self.n_slots

        ret = self.next_server[request_hash]
        if len(self.servers) == 0 or ret is None:
            raise RuntimeError("No servers alive")

        # Here linear probing is necessary since nearest server is required
        return ret

    # get list of all hostnames of servers
    def getServerList(self):
        return list(self.servers.keys())

    # get remaining servers, i.e. maximum number of servers that can be added
    def remaining(self):
        return (self.n_slots - len(self.server_slots)) // self.n_virtual
