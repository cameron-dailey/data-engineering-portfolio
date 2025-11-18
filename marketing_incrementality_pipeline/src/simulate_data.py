import numpy as np
import pandas as pd
from pathlib import Path

np.random.seed(42)

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data" / "raw"
DATA_DIR.mkdir(parents=True, exist_ok=True)

def simulate_marketing_and_revenue(start_date="2022-01-01", end_date="2023-12-31", n_regions=20, treatment_share=0.5):
    dates = pd.date_range(start=start_date, end=end_date, freq="D")
    regions = [f"R{i:02d}" for i in range(1, n_regions+1)]
    treatment = set(regions[:int(n_regions*treatment_share)])
    channels = ["search","social","display","email"]
    rows=[]
    for region in regions:
        is_treat = int(region in treatment)
        base_rev = np.random.uniform(800,1500)
        trend = np.random.uniform(0.01,0.5)
        for i,date in enumerate(dates):
            dow=date.dayofweek
            weekly = 1.0 + (0.2 if dow in (4,5) else -0.05 if dow==1 else 0.0)
            yearly = 1.0 + 0.1*np.cos(2*np.pi*(date.timetuple().tm_yday/365))
            baseline = (base_rev + i*trend)*weekly*yearly
            total_spend = np.random.gamma(3.0,60.0)
            if not is_treat: total_spend*=np.random.uniform(0.05,0.2)
            shares = np.random.dirichlet(np.ones(len(channels)))
            channel_spend={ch:total_spend*s for ch,s in zip(channels,shares)}
            effect = 0
            if is_treat:
                effect = 1.2*np.sqrt(total_spend)+0.3*channel_spend["search"]+0.15*channel_spend["social"]
            noise=np.random.normal(0, baseline*0.1)
            revenue=max(baseline+effect+noise,0)
            for ch in channels:
                rows.append({"date":date,"region_id":region,"is_treatment":is_treat,"channel":ch,
                             "spend":channel_spend[ch],"revenue":revenue/len(channels)})
    return pd.DataFrame(rows)

def main():
    df=simulate_marketing_and_revenue()
    mkt = df.groupby(["date","region_id","is_treatment","channel"],as_index=False)["spend"].sum()
    rev = df.groupby(["date","region_id","is_treatment"],as_index=False)["revenue"].sum()
    mkt.to_csv(DATA_DIR/"marketing_spend_raw.csv",index=False)
    rev.to_csv(DATA_DIR/"revenue_raw.csv",index=False)

if __name__=="__main__":
    main()
