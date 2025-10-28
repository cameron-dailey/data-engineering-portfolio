# SteelCity IoT Maintenance Alerts — Real-Time Jet Ski Telemetry Pipeline

A production-style data engineering project that simulates IoT telemetry streaming from a jet ski fleet, processes messages in real time with Kafka, persists to Postgres, triggers anomaly alerts, and visualizes status in a Streamlit dashboard.

**Highlights**
- Real-time streaming with Kafka (KRaft mode, no Zookeeper)
- Python producer + consumer with rules-based anomaly detection
- PostgreSQL warehouse with indexes
- Streamlit + Plotly dashboard for live monitoring and maps
- Docker Compose spins up Kafka and Postgres quickly

## Architecture
```
[Producer] --Kafka--> [Consumer -> Postgres] --SQL--> [Streamlit Dashboard]
```

**Topic**: `jetski_telemetry`  
**Tables**: `telemetry`, `alerts`

## Tech Stack
- Kafka (bitnami/kafka, KRaft)
- Python: kafka-python, SQLAlchemy, psycopg2-binary
- PostgreSQL 15
- Streamlit + Plotly

## Quickstart

1) Clone & configure
```bash
cp .env.example .env
```

2) Start infra
```bash
docker compose up -d
```

3) Python env
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

4) Init DB
```bash
python db/init_db.py
```

5) Run streaming apps (separate terminals)
```bash
python kafka/producer.py
python kafka/consumer.py
```

6) Dashboard
```bash
streamlit run streamlit_app/app.py
```

## Configuration
Environment variables via `.env`:
- `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`)
- `KAFKA_TOPIC` (default `jetski_telemetry`)
- `DATABASE_URL` (default `postgresql+psycopg2://steelcity:steelcity@localhost:5432/telemetry`)
- `NUM_JETSKIS`, `PRODUCER_INTERVAL_SECONDS`

## Extensions
- Add Airflow for orchestration
- Prometheus + Grafana
- IsolationForest anomaly detection
- Partitions & retention policy
