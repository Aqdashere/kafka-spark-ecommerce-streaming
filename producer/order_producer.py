from kafka import KafkaProducer
import time
import os

base_dir = r"C:\Users\level\Desktop\Coding Projects\University Projects\Big Data\kafka_ecommerce"
csv_path = os.path.join(base_dir, "data", "orders_1000.csv")

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: v.encode("utf-8")
)

with open(csv_path, "r", encoding="utf-8") as f:
    header = next(f) 
    for line in f:
        line = line.strip()
        if not line:
            continue
        producer.send("orders", value=line)
        print("Sent:", line)
        time.sleep(0.1)

producer.flush()
print("Done producing.")
