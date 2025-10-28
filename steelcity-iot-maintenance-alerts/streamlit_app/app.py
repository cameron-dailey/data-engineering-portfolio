import os
import pandas as pd
from sqlalchemy import create_engine
import streamlit as st
import plotly.express as px
from streamlit_autorefresh import st_autorefresh

# --- Page Config ---
st.set_page_config(
    page_title="Steel City IoT Maintenance Dashboard",
    page_icon="🚤",
    layout="wide"
)

# --- Auto-refresh every 10 seconds ---
st_autorefresh(interval=10 * 1000, key="data_refresh")

# --- Database Connection ---
DB_USER = os.getenv("DB_USER", "steelcity")
DB_PASS = os.getenv("DB_PASS", "steelcity")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5433")
DB_NAME = os.getenv("DB_NAME", "telemetry")

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# --- Header ---
st.title("⚡ Steel City IoT: Real-Time Jet Ski Telemetry")
st.caption("Dashboard auto-refreshes every 10 seconds for live telemetry.")

# --- Query Latest Data ---
@st.cache_data(ttl=5)
def get_latest_data():
    query = """
        SELECT *
        FROM telemetry
        ORDER BY ts DESC
        LIMIT 200;
    """
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df

try:
    df = get_latest_data()
except Exception as e:
    st.error(f"Database connection error: {e}")
    st.stop()

if df.empty:
    st.warning("No telemetry data found yet. Waiting for incoming messages...")
    st.stop()

# --- Data Prep ---
latest = (
    df.sort_values("ts", ascending=False)
    .groupby("jetski_id")
    .first()
    .reset_index()
)

# --- Layout ---
col1, col2 = st.columns([2, 1])

# --- Charts ---
with col1:
    st.subheader("Engine Temperature Trends (°C)")
    fig_temp = px.line(
        df,
        x="ts",
        y="engine_temp",
        color="jetski_id",
        labels={"engine_temp": "Temperature (°C)", "ts": "Timestamp"},
        title="Jet Ski Engine Temperature Over Time",
    )
    st.plotly_chart(fig_temp, use_container_width=True)

    st.subheader("RPM vs Battery Voltage")
    fig_rpm = px.scatter(
        df,
        x="rpm",
        y="battery_voltage",
        color="jetski_id",
        size="fuel_level",
        hover_data=["ts"],
        labels={"rpm": "RPM", "battery_voltage": "Battery (V)"},
        title="RPM vs Battery Voltage by Jet Ski",
    )
    st.plotly_chart(fig_rpm, use_container_width=True)

# --- Latest Readings + Map ---
with col2:
    st.subheader("Latest Readings")
    st.dataframe(
        latest[
            [
                "jetski_id",
                "engine_temp",
                "rpm",
                "battery_voltage",
                "fuel_level",
                "latitude",
                "longitude",
            ]
        ],
        hide_index=True,
        use_container_width=True
    )

    st.subheader("Map View (Last Known Location)")
    fig_map = px.scatter_mapbox(
        latest,
        lat="latitude",
        lon="longitude",
        color="jetski_id",
        size="fuel_level",
        hover_name="jetski_id",
        zoom=11,
        mapbox_style="open-street-map",
    )
    st.plotly_chart(fig_map, use_container_width=True)

st.success("✅ Live data loaded successfully!")
