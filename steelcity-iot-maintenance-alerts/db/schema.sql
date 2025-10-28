CREATE TABLE IF NOT EXISTS telemetry (
    id SERIAL PRIMARY KEY,
    event_ts TIMESTAMP NOT NULL,
    jetski_id INT NOT NULL,
    engine_temp DOUBLE PRECISION,
    rpm DOUBLE PRECISION,
    battery_voltage DOUBLE PRECISION,
    fuel_level DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_telemetry_ts ON telemetry(event_ts);
CREATE INDEX IF NOT EXISTS idx_telemetry_jetski_ts ON telemetry(jetski_id, event_ts DESC);

CREATE TABLE IF NOT EXISTS alerts (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP NOT NULL,
    jetski_id INT NOT NULL,
    alert_type TEXT NOT NULL,
    message TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_alerts_created_at ON alerts(created_at);
CREATE INDEX IF NOT EXISTS idx_alerts_jetski_created_at ON alerts(jetski_id, created_at DESC);
