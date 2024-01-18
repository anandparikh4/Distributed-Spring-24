from hash_ds import ConsistentHashMap

ds = ConsistentHashMap()

for i in range(10):
    ds.add(f"Server-{i+4}")

print(ds.getServerList())
print(ds.servers)

print("Slots:")
for idx in range(len(ds.slots)):
    print(idx , ds.slots[idx])
