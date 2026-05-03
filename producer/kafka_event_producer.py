import json
import time
import random
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

channels = ["web", "mobile", "store"]
skus = [f"SKU-{i:03d}" for i in range(100, 200)]

customer_types = ["member", "guest"]
payment_methods = ["credit_card", "debit_card", "cash", "gift_card"]

def base_event():
    return {
        "event_id": str(uuid.uuid4()),
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "store_id": random.randint(1, 20),
        "sku": random.choice(skus),
        "qty": random.randint(1, 5),
        "price": round(random.uniform(5, 500), 2),
        "channel": random.choice(channels),
    }

def generate_event():
    event = base_event()

    # Simulate schema evolution
    # v1_old      -> original schema only
    # v2_partial  -> one new field added
    # v2_full     -> both new fields added
    schema_mode = random.choices(
        ["v1_old", "v2_partial", "v2_full"],
        weights=[0, 0, 100],
        k=1
    )[0]

    if schema_mode == "v2_partial":
        event["customer_type"] = random.choice(customer_types)

    elif schema_mode == "v2_full":
        event["customer_type"] = random.choice(customer_types)
        event["payment_method"] = random.choice(payment_methods)

    return event

while True:
    event = generate_event()
    producer.send("events_raw", event)
    print("Sent:", event)
    time.sleep(1)