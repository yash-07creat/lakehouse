# synthetic_generator.py
import json
import random
from datetime import datetime, timedelta
import os

RAW_DIR = "data/raw"
os.makedirs(RAW_DIR, exist_ok=True)

n = 2000
start = datetime.utcnow() - timedelta(days=7)

for i in range(n):
    rec = {
        "ride_id": f"ride_{i}",
        "pickup_ts": (start + timedelta(seconds=random.randint(0, 7*24*3600))).isoformat() + "Z",
        "dropoff_ts": None,
        "passenger_count": random.randint(1,4),
        "trip_distance": round(random.random()*15, 2),
        "fare_amount": round(2.5 + random.random()*50, 2),
        "payment_type": random.choice(["card","cash","app"]),
        "pickup_borough": random.choice(["Manhattan","Brooklyn","Queens","Bronx","Staten Island"])
    }
    rec["dropoff_ts"] = (datetime.fromisoformat(rec["pickup_ts"].replace("Z","")) + timedelta(minutes=random.randint(5,60))).isoformat() + "Z"

    filename = os.path.join(RAW_DIR, f"ride_{i}.json")
    with open(filename, "w") as f:
        f.write(json.dumps(rec))

print(f"Wrote {n} JSON files to {RAW_DIR}")
