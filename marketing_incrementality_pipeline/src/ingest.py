from pathlib import Path
import pandas as pd

BASE_DIR=Path(__file__).resolve().parents[1]
RAW=BASE_DIR/"data"/"raw"
PROC=BASE_DIR/"data"/"processed"
PROC.mkdir(parents=True,exist_ok=True)

def ingest():
    m=pd.read_csv(RAW/"marketing_spend_raw.csv",parse_dates=["date"])
    r=pd.read_csv(RAW/"revenue_raw.csv",parse_dates=["date"])
    m.to_csv(PROC/"marketing_spend_stage.csv",index=False)
    r.to_csv(PROC/"revenue_stage.csv",index=False)

if __name__=="__main__":
    ingest()
