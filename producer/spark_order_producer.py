from kafka import KafkaProducer
import time
import os
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH = os.path.join(BASE_DIR, "data", "orders_100k.csv")

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: v.encode("utf-8"),
    request_timeout_ms=30000,
    retries=3,
    linger_ms=0,
    acks=1
)

print("Producer connected. Starting to send orders...")

count = 0

with open(CSV_PATH, "r", encoding="utf-8") as f:
    header = next(f)
    for line in f:
        parts = line.strip().split(",")
        if len(parts) != 7:
            continue

        parts[-1] = datetime.now().isoformat()
        message = ",".join(parts)

        producer.send("orders", message)
        count += 1

        if count % 1000 == 0:
            print(f"Sent {count} orders")

        time.sleep(0.005)  
producer.flush()
producer.close()
print(f"Finished sending {count} orders.")
