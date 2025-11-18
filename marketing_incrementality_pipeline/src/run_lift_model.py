from pathlib import Path
import pandas as pd
import numpy as np

BASE_DIR=Path(__file__).resolve().parents[1]
PROC=BASE_DIR/"data"/"processed"

def estimate_incrementality():
    df=pd.read_csv(PROC/"fact_marketing_daily.csv",parse_dates=["date"])
    agg=df.groupby(["date","is_treatment"],as_index=False)[["channel_spend","revenue"]].sum()
    agg=agg.rename(columns={"channel_spend":"total_spend"})
    control=agg[agg["is_treatment"]==0]
    treat=agg[agg["is_treatment"]==1]
    merged=treat.merge(control[["date","revenue"]].rename(columns={"revenue":"control_revenue"}),on="date")
    merged["baseline_revenue"]=merged["control_revenue"]
    merged["incremental_revenue_daily"]=merged["revenue"]-merged["baseline_revenue"]
    total_inc=merged["incremental_revenue_daily"].sum()
    total_rev=merged["revenue"].sum()
    total_base=merged["baseline_revenue"].sum()
    total_spend=merged["total_spend"].sum()
    summary=pd.DataFrame([
        {"metric":"total_incremental_revenue","value":total_inc},
        {"metric":"incrementality_pct","value":total_inc/total_rev},
        {"metric":"incremental_roas","value":total_inc/total_spend},
        {"metric":"total_spend","value":total_spend},
    ])
    summary.to_csv(PROC/"incrementality_summary.csv",index=False)
    merged.to_csv(PROC/"incrementality_daily_timeseries.csv",index=False)

if __name__=="__main__":
    estimate_incrementality()
