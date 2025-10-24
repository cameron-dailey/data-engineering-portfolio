import pandas as pd
from common.etl_utils import get_engine, run_sql, upsert_dataframe

def main():
    engine = get_engine()

    # Step 1: Combine features from bookings, weather, and maintenance
    print("Building features_usage_weather...")

    run_sql("""
        DROP TABLE IF EXISTS features_usage_weather;
        CREATE TABLE features_usage_weather AS
        SELECT
          b.jet_ski_id,
          DATE(b.start_time) AS day,
          SUM(b.duration_hours) AS hours_used_day,
          AVG(w.temp_c) AS avg_temp_c,
          AVG(w.precip_mm) AS avg_precip_mm,
          AVG(w.wind_kmh) AS avg_wind_kmh
        FROM bookings b
        LEFT JOIN weather w
          ON DATE(w.datetime) = DATE(b.start_time)
          AND EXTRACT(HOUR FROM w.datetime) = EXTRACT(HOUR FROM b.start_time)
        GROUP BY 1,2;
    """, engine)

    print("Created features_usage_weather table.")

    # Optional: verify results
    df = pd.read_sql("SELECT * FROM features_usage_weather LIMIT 5;", engine)
    print(df)

if __name__ == "__main__":
    main()