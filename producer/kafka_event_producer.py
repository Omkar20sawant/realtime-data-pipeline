import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

channels = ["web", "mobile", "store"]
skus = [f"SKU-{i:03d}" for i in range(100, 200)]

def generate_event():
    return {
        "event_id": str(random.randint(100000, 999999)),
        "event_ts": datetime.utcnow().isoformat(),
        "store_id": random.randint(1, 20),
        "sku": random.choice(skus),
        "qty": random.randint(1, 5),
        "price": round(random.uniform(5, 500), 2),
        "channel": random.choice(channels),
    }

while True:
    event = generate_event()
    producer.send("events_raw", event)
    print("Sent:", event)
    time.sleep(1)