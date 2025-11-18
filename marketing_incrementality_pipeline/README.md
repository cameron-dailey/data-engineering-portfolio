#  Marketing Incrementality Pipeline  
*A full data engineering + marketing analytics project implementing experiment simulation, ETL pipelines, dbt modeling, and an interactive Streamlit dashboard.*

---

##  Project Overview

This project simulates a geo-based marketing experiment, processes the data through a complete ETL pipeline, models incremental revenue lift, and visualizes results in a fully interactive dashboard.

It demonstrates real-world skills across:

- **Data Engineering** (pipelines, dbt, modular code, orchestration-ready structure)
- **Data Analytics** (revenue modeling, metrics, lift analysis)
- **Experimentation & Causal Inference** (incrementality estimation)
- **App Development** (Streamlit front-end dashboard)
- **Software Engineering** (clean structure, reusability, versioning)

This is designed as a *portfolio-quality project* suitable for data engineering, analytics engineer, and marketing science roles.

---

##  What This Project Does

###  1. **Simulates a real marketing experiment**
- Multiple regions  
- Treatment vs Control assignment  
- Channel spend (search, social, display, email)  
- Seasonality, noise, baseline trend  
- Revenue generation with causal lift  

###  2. **Runs a full ETL pipeline**
- Raw → Staging → Processed layers  
- Modular scripts (`simulate_data.py`, `ingest.py`, `transform.py`, `run_lift_model.py`)
- Clean directory structure

###  3. **Builds analytical models with dbt**
- Staging models  
- Dimensional models  
- Fact tables  
- Marts for incremental reporting  
- DuckDB backend

###  4. **Computes lift and incrementality**
- Daily incremental revenue  
- Baseline revenue estimation  
- Incremental ROAS  
- Total incremental revenue  

###  5. **Interactive dashboard**
Built with Plotly + Streamlit:

- KPI metric tiles  
- Revenue vs Baseline chart  
- Daily lift chart  
- Cumulative lift chart  
- Date range filtering  
- Responsive UI  

---


