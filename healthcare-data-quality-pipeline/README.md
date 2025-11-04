# Healthcare Data Quality Pipeline (Kafka + Postgres + Airflow)

A realistic **healthcare operations** data pipeline that streams patient-flow events (admit, transfer, discharge) into a warehouse and enforces **data quality** with automated validations.

## Stack
- **Kafka** + **Zookeeper** for streaming
- **PostgreSQL** for staging and warehouse schemas
- **Python** for producer, consumer, and ETL
- **Airflow** to orchestrate ETL and validation
- **YAML-driven validations** (Great Expectations–style)
- **Docker Compose** for reproducible local setup

## Architecture
```
Kafka Producer -> Kafka Topic (patient_events) -> Kafka Consumer -> Postgres (staging)
Airflow (cron) -> Transform script -> Postgres (warehouse) -> Validation (YAML checks)
```

## Quickstart

### 1) Prereqs
- Docker + Docker Compose
- Optional: Python 3.10+ if you want to run scripts from your host

### 2) Environment
```bash
cp .env.example .env
# Edit .env if needed
```

### 3) Start core services
```bash
docker compose up -d zookeeper kafka postgres
```

### 4) Produce and consume events
In one terminal:
```bash
export $(cat .env | xargs)
python kafka/producer.py
```
In another terminal:
```bash
export $(cat .env | xargs)
python kafka/consumer.py
```

Events land in `staging.patient_events_raw`.

### 5) Run ETL (via Airflow or directly)

**Airflow (recommended):**
```bash
docker compose up -d airflow
# Airflow UI at http://localhost:8080 (admin / admin)
# Enable DAGs: healthcare_flow_etl, data_quality_validation
```

**Direct from host:**
```bash
export $(cat .env | xargs)
python scripts/transform_patient_data.py
```

### 6) Run Data Quality Validation
```bash
export $(cat .env | xargs)
python scripts/run_gx_validation.py
```

The validator checks:
- Row count > 0
- `patient_id` exists and has no nulls
- `event_type` in {'admit','transfer','discharge'}
- `event_time` not null
- Logical time rule: `first_admit < last_discharge` in `warehouse.patient_stay_summary`

## Schemas
- `staging.patient_events_raw` – raw event payloads (JSONB kept for lineage)
- `warehouse.patients` – unique patients
- `warehouse.events` – normalized event history
- `warehouse.patient_stay_summary` – materialized view with stay duration (hours)

## Notes
- Airflow uses **SequentialExecutor** and SQLite for metadata to keep setup simple.
- If Airflow needs extra packages, it installs from `airflow/requirements_airflow.txt` at container start.
- On non-Linux hosts, the DAGs use `host.docker.internal` to reach your local Postgres; adjust `DATABASE_URL` if needed.

## Ideas to Extend
- Swap the YAML validator for full **Great Expectations** Data Docs + Checkpoints (HTML reports)
- Add **pytest** unit tests and **GitHub Actions CI** to run validations on PR
- Add **Grafana/Prometheus** for pipeline metrics/alerts
- Add **Spark** or **dbt** for scalable transformations
