import streamlit as st
import pandas as pd
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go

# ----------------------------------
# Paths & Data Loader
# ----------------------------------
BASE = Path(__file__).resolve().parents[1]
PROC = BASE / "data" / "processed"

@st.cache_data
def load_data():
    summary = pd.read_csv(PROC / "incrementality_summary.csv")
    daily = pd.read_csv(PROC / "incrementality_daily_timeseries.csv", parse_dates=["date"])
    return summary, daily


# ----------------------------------
# Main App
# ----------------------------------
def main():
    st.set_page_config(
        page_title="Incrementality Dashboard",
        layout="wide"
    )
    
    st.title("📈 Marketing Incrementality Dashboard")
    
    # Load data
    try:
        summary, daily = load_data()
    except Exception as e:
        st.error("❌ Data not found. Run the pipeline first.")
        st.write(e)
        return
    
    # Add cumulative revenue
    daily["cum"] = daily["incremental_revenue_daily"].cumsum()
    daily = daily.sort_values("date")

    # ----------------------------
    # Sidebar Filters
    # ----------------------------
    st.sidebar.header("🔍 Filters")

    min_date = daily["date"].min()
    max_date = daily["date"].max()

    date_range = st.sidebar.date_input(
        "Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )

    if len(date_range) == 2:
        start, end = date_range
        daily = daily[(daily["date"] >= pd.to_datetime(start)) & (daily["date"] <= pd.to_datetime(end))]

    # ----------------------------
    # KPI Cards
    # ----------------------------
    st.subheader("Key Metrics")

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("💰 Total Incremental Revenue", f"${summary.loc[0, 'value']:,.0f}")
    col2.metric("📈 Incrementality %", f"{summary.loc[1, 'value']*100:.2f}%")
    col3.metric("🔁 Incremental ROAS", f"{summary.loc[2, 'value']:.2f}")
    col4.metric("💸 Total Spend", f"${summary.loc[3, 'value']:,.0f}")

    st.markdown("---")

    # ----------------------------
    # 1. Revenue vs Baseline
    # ----------------------------
    st.subheader("1. Revenue vs Baseline")

    fig1 = go.Figure()

    fig1.add_trace(go.Scatter(
        x=daily["date"],
        y=daily["baseline_revenue"],
        mode="lines",
        name="Baseline Revenue",
        line=dict(color="#1f77b4", width=2)
    ))

    fig1.add_trace(go.Scatter(
        x=daily["date"],
        y=daily["revenue"],
        mode="lines",
        name="Revenue",
        line=dict(color="#ff7f0e", width=2)
    ))

    fig1.update_layout(
        height=400,
        template="plotly_white",
        xaxis_title="Date",
        yaxis_title="Revenue ($)",
        legend=dict(orientation="h", y=1.15)
    )

    st.plotly_chart(fig1, use_container_width=True)

    # ----------------------------
    # 2. Daily Incremental Revenue
    # ----------------------------
    st.subheader("2. Daily Incremental Revenue")

    fig2 = px.line(
        daily,
        x="date",
        y="incremental_revenue_daily",
        labels={"incremental_revenue_daily": "Incremental Revenue ($)"},
        template="plotly_white"
    )

    fig2.update_traces(line_color="#2ca02c")
    fig2.update_layout(height=350)

    st.plotly_chart(fig2, use_container_width=True)

    # ----------------------------
    # 3. Cumulative Incremental Revenue
    # ----------------------------
    st.subheader("3. Cumulative Incremental Revenue")

    fig3 = px.line(
        daily,
        x="date",
        y="cum",
        labels={"cum": "Cumulative Incremental Revenue ($)"},
        template="plotly_white"
    )

    fig3.update_traces(line_color="#9467bd", line_width=3)
    fig3.update_layout(height=350)

    st.plotly_chart(fig3, use_container_width=True)


# ----------------------------------
# Run App
# ----------------------------------
if __name__ == "__main__":
    main()
