import pandas as pd
from common.etl_utils import get_engine, upsert_dataframe

def main():
    engine = get_engine()
    df = pd.read_csv("data/weather.csv", parse_dates=["datetime"])
    upsert_dataframe(df, "weather", engine, if_exists="replace")
    print(f"Ingested {len(df)} rows into weather.")

if __name__ == "__main__":
    main()
