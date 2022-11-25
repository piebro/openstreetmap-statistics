import re
import random

with open('assets/data.js') as f:
    datajs = "\n".join(f.readlines())
exisiting_hashes = re.findall(r"url_hash:\"([a-f0-9]{4})\",", datajs)

new_hash = f"{random.randint(0, 16*16*16*16):x}"
while new_hash in exisiting_hashes:
    new_hash = f"{random.randint(0, 16*16*16*16):x}"

print(new_hash)