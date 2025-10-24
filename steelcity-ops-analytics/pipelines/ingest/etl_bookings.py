import pandas as pd
from common.etl_utils import get_engine, upsert_dataframe, run_sql

def main():
    engine = get_engine()
    df = pd.read_csv("data/bookings.csv", parse_dates=["start_time"])
    upsert_dataframe(df, "bookings", engine, if_exists="replace")
    print(f"Ingested {len(df)} rows into bookings.")

    # Build/refresh derived
    run_sql("""DROP TABLE IF EXISTS bookings_hourly;""", engine)
    run_sql("""
    CREATE TABLE bookings_hourly AS
    SELECT
      DATE(start_time) AS day,
      EXTRACT(HOUR FROM start_time) AS hour,
      river,
      COUNT(*) AS total_bookings,
      SUM(duration_hours) AS total_hours,
      SUM(price_usd) AS revenue_usd
    FROM bookings
    GROUP BY 1,2,3;
    """, engine)
    print("Refreshed bookings_hourly.")

if __name__ == "__main__":
    main()