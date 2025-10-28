import os
import json
import time
import random
from datetime import datetime
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
topic = os.getenv("KAFKA_TOPIC", "jetski_telemetry")
num_jetskis = int(os.getenv("NUM_JETSKIS", 12))
interval = float(os.getenv("PRODUCER_INTERVAL_SECONDS", 2))

producer_conf = {'bootstrap.servers': bootstrap_servers}
producer = Producer(producer_conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message sent to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

def generate_telemetry():
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "jetski_id": random.randint(1, num_jetskis),
        "engine_temp": round(random.uniform(70, 110), 2),
        "rpm": round(random.uniform(3000, 8000), 2),
        "battery_voltage": round(random.uniform(10.5, 13.5), 2),
        "fuel_level": round(random.uniform(10, 100), 2),
        "latitude": round(random.uniform(40.44, 40.48), 6),
        "longitude": round(random.uniform(-80.02, -79.97), 6)
    }

print(f"Producing to topic '{topic}' on {bootstrap_servers}")

while True:
    msg = json.dumps(generate_telemetry())
    producer.produce(topic, msg.encode('utf-8'), callback=delivery_report)
    producer.poll(0)
    print(f"Produced message: {msg}")
    time.sleep(interval)
