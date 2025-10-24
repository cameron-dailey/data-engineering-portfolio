import os
import pandas as pd
import plotly.express as px
import joblib
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import streamlit as st

# -----------------------------------------------------------------------------
# Load environment and connect to Postgres inside Docker
# -----------------------------------------------------------------------------
load_dotenv()
DB_URL = os.getenv("DATABASE_URL", "postgresql://steelcity:steelcity@db:5432/steelcity")
engine = create_engine(DB_URL, future=True)

# -----------------------------------------------------------------------------
# Streamlit layout setup
# -----------------------------------------------------------------------------
st.set_page_config(page_title="Steel City Ops Analytics", layout="wide")
st.title("Steel City Jet Ski — Operational Analytics")

tab1, tab2, tab3 = st.tabs(["KPIs & Trends", "Weather Impact", "Maintenance Risk"])

# -----------------------------------------------------------------------------
# TAB 1: KPIs & Trends
# -----------------------------------------------------------------------------
with tab1:
    st.subheader("Daily KPIs")

    kpi = pd.read_sql_query(text("SELECT * FROM kpi_daily ORDER BY day"), engine, parse_dates=["day"])
    if kpi.empty:
        st.warning("No KPI data found. Make sure to run pipelines/transform/build_features.py first.")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Bookings", int(kpi["bookings"].sum()))
        c2.metric("Total Revenue (USD)", f"${kpi['revenue_usd'].sum():,.0f}")
        c3.metric("Avg Order Value", f"${kpi['avg_order_value'].mean():.2f}")

        fig_b = px.line(kpi, x="day", y="bookings", title="Bookings per Day")
        st.plotly_chart(fig_b, use_container_width=True)

        fig_r = px.line(kpi, x="day", y="revenue_usd", title="Revenue per Day (USD)")
        st.plotly_chart(fig_r, use_container_width=True)

        hourly = pd.read_sql_query(
            text("SELECT * FROM bookings_hourly ORDER BY day, hour"), engine, parse_dates=["day"]
        )
        if not hourly.empty:
            fig_h = px.density_heatmap(
                hourly,
                x="hour",
                y="river",
                z="total_bookings",
                title="Hourly Bookings Heatmap",
                nbinsx=12,
                histfunc="avg",
            )
            st.plotly_chart(fig_h, use_container_width=True)
        else:
            st.info("No hourly booking data available yet.")

# -----------------------------------------------------------------------------
# TAB 2: Weather Impact
# -----------------------------------------------------------------------------
with tab2:
    st.subheader("Weather vs Bookings (Daily)")
    q = """
    WITH daily_w AS (
      SELECT DATE(datetime) AS day,
             AVG(temp_c) AS avg_temp_c,
             AVG(precip_mm) AS avg_precip_mm,
             AVG(wind_kmh) AS avg_wind_kmh
      FROM weather GROUP BY 1
    )
    SELECT k.day, k.bookings, k.revenue_usd, d.avg_temp_c, d.avg_precip_mm, d.avg_wind_kmh
    FROM kpi_daily k
    LEFT JOIN daily_w d ON d.day = k.day
    ORDER BY k.day;
    """
    wx = pd.read_sql_query(text(q), engine, parse_dates=["day"])
    if wx.empty:
        st.warning("No weather or KPI data found. Run pipelines/ingest and transform first.")
    else:
        fig_sc = px.scatter(wx, x="avg_temp_c", y="bookings", trendline="ols", title="Bookings vs Avg Temp (°C)")
        st.plotly_chart(fig_sc, use_container_width=True)

        fig_sc2 = px.scatter(wx, x="avg_precip_mm", y="bookings", trendline="ols", title="Bookings vs Precip (mm)")
        st.plotly_chart(fig_sc2, use_container_width=True)

# -----------------------------------------------------------------------------
# TAB 3: Maintenance Risk
# -----------------------------------------------------------------------------
with tab3:
    st.subheader("Maintenance Risk (Next 7 Days)")
    feat = pd.read_sql_query(
        text("SELECT * FROM features_usage_weather ORDER BY day DESC LIMIT 200"),
        engine,
        parse_dates=["day"],
    )

    if feat.empty:
        st.warning("No feature data found. Run pipelines/transform/build_features.py first.")
    else:
        feat = feat.fillna(0)
        model_path = "models/maintenance_rf.pkl"

        if os.path.exists(model_path):
            clf = joblib.load(model_path)
            X = feat[["hours_used_day", "avg_temp_c", "avg_precip_mm", "avg_wind_kmh"]]
            feat["risk_score"] = clf.predict_proba(X)[:, 1]

            st.dataframe(feat.sort_values(["day", "risk_score"], ascending=[False, False]))
            st.caption("Risk score = probability of maintenance event within next 7 days.")
        else:
            st.warning("Train the model first: `python analytics/train_maintenance_model.py`")
st.sidebar.write("""
1. Run the three ETL scripts in **pipelines/ingest** to load sample data.
2. Run **pipelines/transform/build_features.py** to compute KPIs and features.
3. Train the model via **analytics/train_maintenance_model.py**.
4. Launch this app: `streamlit run analytics/dashboards/streamlit_app.py`.
""")
