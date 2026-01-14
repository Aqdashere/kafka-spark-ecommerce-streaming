from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "alerts",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("Listening for alerts...\n")

for msg in consumer:
    print("🚨 ALERT:", msg.value)
