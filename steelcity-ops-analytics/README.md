# Steel City Jet Ski — Operational Analytics Platform

End-to-end, production-style data engineering and analytics project built on realistic jet ski rental data.  
Includes ingestion (ETL), transformations, feature engineering, a maintenance prediction model, and an interactive Streamlit dashboard.

---

## What This Demonstrates

**Data Engineering:**  
ETL pipelines ingest CSVs → load into SQLite/Postgres → apply transformations and feature generation.  

**Analytics & ML:**  
Compute KPIs (bookings/hour, revenue, utilization), analyze weather impact, and predict maintenance risk using a Random Forest model.  

**Reproducibility:**  
Clear folder structure, requirements file, and `.env` sample make the project one-command deployable.

---

## Tech Stack

- **Languages:** Python, SQL  
- **Libraries:** Pandas, SQLAlchemy, scikit-learn, Plotly, Streamlit  
- **Database:** SQLite (default) or PostgreSQL (via `DATABASE_URL`)  
- **Environment:** Docker or local Python venv  

---

## Quickstart (Local)

```bash
# 1. Create environment
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp env.sample .env

# 2. Load sample data
python pipelines/ingest/etl_bookings.py
python pipelines/ingest/etl_weather.py
python pipelines/ingest/etl_maintenance.py

# 3. Build derived tables + features
python pipelines/transform/build_features.py

# 4. Train maintenance model
python analytics/train_maintenance_model.py

# 5. Launch dashboard
streamlit run analytics/dashboards/streamlit_app.py
