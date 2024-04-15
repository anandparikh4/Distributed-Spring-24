import json

with open("data.json", "r") as f:
    data = json.load(f)
    
data = data["data"]
ids = [d["stud_id"] for d in data]
ids.sort()

print(ids)
