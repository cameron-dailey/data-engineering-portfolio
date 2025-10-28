import os
import json
import time
from confluent_kafka import Consumer
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import sys
sys.stdout.reconfigure(encoding='utf-8')

# Load environment variables
load_dotenv()

# ---------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "jetski_telemetry")
DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://steelcity:password@localhost:5433/telemetry"
)

# Create SQLAlchemy engine for PostgreSQL
engine = create_engine(DB_URL)


# ---------------------------------------------------------------------
# MAIN CONSUMER LOOP
# ---------------------------------------------------------------------
def main():
    print(f"\nListening to topic '{TOPIC}' on {KAFKA_BROKER}...\n")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "telemetry-group",
        "auto.offset.reset": "earliest"
    })

    consumer.subscribe([TOPIC])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            try:
                data = json.loads(msg.value().decode("utf-8"))
                # Rename the key to match DB column name
                data["ts"] = data.pop("timestamp")

                with engine.begin() as conn:
                    conn.execute(text("""
                        INSERT INTO telemetry (
                            ts, jetski_id, engine_temp, rpm,
                            battery_voltage, fuel_level, latitude, longitude
                        ) VALUES (
                            :ts, :jetski_id, :engine_temp, :rpm,
                            :battery_voltage, :fuel_level, :latitude, :longitude
                        )
                    """), data)

                print(
                    f"✅ Inserted → JetSki {data['jetski_id']} | "
                    f"Temp {data['engine_temp']}°C | Battery {data['battery_voltage']}V"
                )

            except Exception as e:
                print(f"❌ Error processing message: {e}")

    except KeyboardInterrupt:
        print("\nShutting down consumer...")
    finally:
        consumer.close()
        engine.dispose()
        print("🟢 Consumer stopped cleanly.")


# ---------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------
if __name__ == "__main__":
    while True:
        try:
            main()
        except Exception as e:
            print(f"⚠️ Consumer loop error: {e}")
            time.sleep(5)
