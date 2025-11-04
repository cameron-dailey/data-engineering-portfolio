import os, json, logging
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "patient_events")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://hdqp:hdqp@localhost:5432/hdqp")

SCHEMA_STAGING = "staging"

DDL = f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA_STAGING};
CREATE TABLE IF NOT EXISTS {SCHEMA_STAGING}.patient_events_raw (
    id BIGSERIAL PRIMARY KEY,
    patient_id BIGINT,
    event_type TEXT,
    unit TEXT,
    bed_id TEXT,
    event_time TIMESTAMPTZ,
    mrn TEXT,
    raw JSONB,
    ingested_at TIMESTAMPTZ DEFAULT now()
);
"""

def ensure_tables(engine):
    with engine.begin() as conn:
        conn.execute(text(DDL))

def run():
    engine = create_engine(DATABASE_URL, future=True)
    ensure_tables(engine)
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    logging.info(f"Consuming from %s on %s ...", TOPIC, BOOTSTRAP)
    for msg in consumer:
        payload = msg.value
        with engine.begin() as conn:
            conn.execute(
                text(f"""
                INSERT INTO {SCHEMA_STAGING}.patient_events_raw
                    (patient_id, event_type, unit, bed_id, event_time, mrn, raw)
                VALUES
                    (:patient_id, :event_type, :unit, :bed_id, :event_time, :mrn, to_jsonb(:raw::json))
                """),
                {
                    "patient_id": payload.get("patient_id"),
                    "event_type": payload.get("event_type"),
                    "unit": payload.get("unit"),
                    "bed_id": payload.get("bed_id"),
                    "event_time": payload.get("event_time"),
                    "mrn": payload.get("mrn"),
                    "raw": json.dumps(payload),
                }
            )

if __name__ == "__main__":
    run()
