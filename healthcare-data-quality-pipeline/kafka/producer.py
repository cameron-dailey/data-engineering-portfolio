import json, os, random, time
from datetime import datetime, timedelta
from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "patient_events")

EVENT_TYPES = ["admit", "transfer", "discharge"]
UNITS = ["ER", "ICU", "MED", "SURG", "ORTHO", "ONCO", "PED"]
BEDS = [f"{unit}-{i:03d}" for unit in UNITS for i in range(1, 21)]

def rand_dt(hours=72):
    now = datetime.utcnow()
    delta = timedelta(hours=random.randint(0, hours), minutes=random.randint(0, 59))
    return (now - delta).isoformat(timespec="seconds") + "Z"

def gen_event():
    patient_id = random.randint(100000, 999999)
    event_type = random.choices(EVENT_TYPES, weights=[5,3,2])[0]
    bed_id = random.choice(BEDS)
    maybe_null = None if random.random() < 0.01 else patient_id  # inject occasional bad data
    return {
        "patient_id": maybe_null,
        "event_type": event_type,
        "unit": bed_id.split("-")[0],
        "bed_id": bed_id,
        "event_time": rand_dt(),
        "mrn": str(patient_id),
    }

def main():
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP, value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    print(f"Producing to {TOPIC} on {BOOTSTRAP} ... Ctrl+C to stop.")
    try:
        while True:
            producer.send(TOPIC, gen_event())
            producer.flush()
            time.sleep(0.25)
    except KeyboardInterrupt:
        print("Stopped.")

if __name__ == "__main__":
    main()
