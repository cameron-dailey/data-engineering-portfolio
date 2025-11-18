from pathlib import Path
import pandas as pd

BASE_DIR=Path(__file__).resolve().parents[1]
PROC=BASE_DIR/"data"/"processed"

def transform():
    m=pd.read_csv(PROC/"marketing_spend_stage.csv",parse_dates=["date"])
    r=pd.read_csv(PROC/"revenue_stage.csv",parse_dates=["date"])
    fact=m.merge(r,on=["date","region_id","is_treatment"],how="left").rename(columns={"spend":"channel_spend"})
    fact.to_csv(PROC/"fact_marketing_daily.csv",index=False)

if __name__=="__main__":
    transform()
