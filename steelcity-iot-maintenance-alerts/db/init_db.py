from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()
database_url = os.getenv("DATABASE_URL")
engine = create_engine(database_url, echo=True)

def main():
    with engine.begin() as conn:
        conn.execute(text("""
            DROP TABLE IF EXISTS telemetry;
            CREATE TABLE telemetry (
                id SERIAL PRIMARY KEY,
                ts TIMESTAMP,
                jetski_id INT,
                engine_temp FLOAT,
                rpm FLOAT,
                battery_voltage FLOAT,
                fuel_level FLOAT,
                latitude FLOAT,
                longitude FLOAT
            );
        """))
    print("Telemetry table created successfully.")

if __name__ == "__main__":
    main()
