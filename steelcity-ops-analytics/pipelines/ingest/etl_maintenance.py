import pandas as pd
from common.etl_utils import get_engine, upsert_dataframe

def main():
    engine = get_engine()
    df = pd.read_csv("data/maintenance.csv", parse_dates=["date"])
    upsert_dataframe(df, "maintenance", engine, if_exists="replace")
    print(f"Ingested {len(df)} rows into maintenance.")

if __name__ == "__main__":
    main()
