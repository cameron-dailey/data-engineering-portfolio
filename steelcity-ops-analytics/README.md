# Steel City Jet Ski — Operational Analytics Platform

End-to-end, production-style data engineering + analytics project using realistic jet ski rental data.
It includes ingestion (ETL), transformations, a maintenance prediction model, and a Streamlit dashboard.

## What this shows
- **Data Engineering:** CSV ingest → SQLite (or Postgres), transformations, feature building, orchestration-ready DAG.
- **Analytics & ML:** KPIs (bookings/hour, revenue, utilization), weather impact, maintenance risk scoring (RandomForest).
- **Reproducible:** Clear folder structure, requirements, sample data, and a one-command local demo.

## Stack
- Python, Pandas, SQLAlchemy, scikit-learn
- SQLite by default (swap to Postgres via `DATABASE_URL`)
- Streamlit + Plotly for dashboard

## Quickstart (local)
```bash
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp env.sample .env

# 1) Load sample data into DB
python pipelines/ingest/etl_bookings.py
python pipelines/ingest/etl_weather.py
python pipelines/ingest/etl_maintenance.py

# 2) Build features + derived tables
python pipelines/transform/build_features.py

# 3) Train maintenance model
python analytics/train_maintenance_model.py

# 4) Run dashboard
streamlit run analytics/dashboards/streamlit_app.py
```

## Project layout
```
steelcity-ops-analytics/
├─ analytics/
│  ├─ dashboards/streamlit_app.py
│  └─ train_maintenance_model.py
├─ common/etl_utils.py
├─ data/ (sample CSVs)
├─ db/schema.sql
├─ models/ (trained model artifacts)
├─ pipelines/
│  ├─ ingest/etl_bookings.py
│  ├─ ingest/etl_weather.py
│  ├─ ingest/etl_maintenance.py
│  ├─ transform/build_features.py
│  └─ orchestration/airflow_dag.py
├─ tests/test_transforms.py
├─ env.sample  (copy to .env)
├─ requirements.txt
└─ README.md
```

## Notes
- The Airflow DAG is illustrative. You can point it to local scripts or adapt to your cloud.
- Swap SQLite for Postgres by changing `DATABASE_URL` and creating the target DB.
- The synthetic data covers ~120 days; feel free to regenerate or extend for demos.
