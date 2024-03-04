from hash_ds import ConsistentHashMap
from hash_functions import requestHashList, serverHashList

ds = ConsistentHashMap(request_hash=requestHashList[1], server_hash=serverHashList[1])

# for i in range(10):
#     ds.add(f"Server-{i+4}")

print(ds.getServerList())
print(ds.servers)

print("Slots:")
for idx in range(len(ds.slots)):
    print(f'{idx:>3}: {ds.slots[idx]}')
