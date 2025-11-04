import os
from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://hdqp:hdqp@localhost:5432/hdqp")

SCHEMA_STAGING = "staging"
SCHEMA_WH = "warehouse"

DDL = f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA_WH};

CREATE TABLE IF NOT EXISTS {SCHEMA_WH}.patients (
    patient_id BIGINT PRIMARY KEY,
    mrn TEXT
);

CREATE TABLE IF NOT EXISTS {SCHEMA_WH}.events (
    event_id BIGSERIAL PRIMARY KEY,
    patient_id BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    unit TEXT,
    bed_id TEXT,
    event_time TIMESTAMPTZ NOT NULL,
    CONSTRAINT fk_patient FOREIGN KEY (patient_id) REFERENCES {SCHEMA_WH}.patients (patient_id)
);

CREATE MATERIALIZED VIEW IF NOT EXISTS {SCHEMA_WH}.patient_stay_summary AS
SELECT
    patient_id,
    MIN(event_time) FILTER (WHERE event_type = 'admit') AS first_admit,
    MAX(event_time) FILTER (WHERE event_type = 'discharge') AS last_discharge,
    EXTRACT(EPOCH FROM (MAX(event_time) FILTER (WHERE event_type = 'discharge') - MIN(event_time) FILTER (WHERE event_type = 'admit'))) / 3600.0
        AS stay_duration_hours
FROM {SCHEMA_WH}.events
GROUP BY patient_id;
"""

UPSERT_PATIENTS = f"""
INSERT INTO {SCHEMA_WH}.patients (patient_id, mrn)
SELECT DISTINCT patient_id, mrn
FROM {SCHEMA_STAGING}.patient_events_raw
WHERE patient_id IS NOT NULL
ON CONFLICT (patient_id) DO UPDATE SET mrn = EXCLUDED.mrn;
"""

INSERT_EVENTS = f"""
INSERT INTO {SCHEMA_WH}.events (patient_id, event_type, unit, bed_id, event_time)
SELECT patient_id, event_type, unit, bed_id, event_time::timestamptz
FROM {SCHEMA_STAGING}.patient_events_raw per
WHERE event_time IS NOT NULL
  AND event_type IN ('admit','transfer','discharge')
  AND patient_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM {SCHEMA_WH}.events e
      WHERE e.patient_id = per.patient_id
        AND e.event_type = per.event_type
        AND e.event_time = per.event_time::timestamptz
  );
"""

REFRESH_SUMMARY = f"""
REFRESH MATERIALIZED VIEW CONCURRENTLY {SCHEMA_WH}.patient_stay_summary;
"""

def run():
    engine = create_engine(DATABASE_URL, future=True)
    with engine.begin() as conn:
        conn.execute(text(DDL))
        conn.execute(text(UPSERT_PATIENTS))
        conn.execute(text(INSERT_EVENTS))
        try:
            conn.execute(text(REFRESH_SUMMARY))
        except Exception:
            conn.execute(text("DROP MATERIALIZED VIEW IF EXISTS warehouse.patient_stay_summary"))
            conn.execute(text(DDL))

if __name__ == "__main__":
    run()
