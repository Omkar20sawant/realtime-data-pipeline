import json
import os
import time
import uuid
import random
from datetime import datetime, timezone

OUT_DIR = os.path.join("data", "raw_events")
os.makedirs(OUT_DIR, exist_ok=True)

STORES = [101,102, 103, 104, 105]
SKUS= [f"SKU{n:03d}" for n in range(1, 51)]
CHANNELS = ["store", "web"]

def make_event():
    qty = random.randint(1,5)
    price = round(random.uniform(5.0, 120.0), 2)
    return{
        "event_id":str(uuid.uuid4()),
        "event_ts":datetime.now(timezone.utc).isoformat(),
        "store_id":random.choice(STORES),
        "channel":random.choice(CHANNELS),
        "sku":random.choice(SKUS),
        "qty":qty,
        "price":price
    }


def write_batch(batch_size: int = 20):
    # Write each batch to a new file so Spark can pick it up as a micro-batch
    file_name = f"events_{int(time.time()*1000)}.json"
    path = os.path.join(OUT_DIR, file_name)

    with open(path, "w") as f:
        for _ in range(batch_size):
            f.write(json.dumps(make_event()) + "\n")
    
    print(f"Wrote {batch_size} events -> {path}")

if __name__ == "__main__":
    batch_size = int(os.environ.get("BATCH_SIZE", "20"))
    interval_sec = float(os.environ.get("INTERVAL_SEC", "2"))

    print("Starting event generator...")
    print(f"Output dir: {OUT_DIR}")
    print(f"Batch size: {batch_size}, Interval: {interval_sec}s")

    while True:
        write_batch(batch_size=batch_size)
        time.sleep(interval_sec)