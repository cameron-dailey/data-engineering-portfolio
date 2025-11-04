from common.etl_utils import get_engine, run_sql

def main():
    engine = get_engine()
    # Create/refresh KPI table
    run_sql("DROP TABLE IF EXISTS kpi_daily;", engine)
    run_sql("""
    CREATE TABLE kpi_daily AS
    SELECT
      DATE(start_time) AS day,
      COUNT(*) AS bookings,
      SUM(price_usd) AS revenue_usd,
      AVG(price_usd) AS avg_order_value,
      SUM(duration_hours) AS hours_total
    FROM bookings
    GROUP BY 1;
    """, engine)

    # Usage + weather features (per ski per day)
    run_sql("DROP TABLE IF EXISTS features_usage_weather;", engine)
    run_sql("""
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
    print("Built kpi_daily and features_usage_weather.")

if __name__ == "__main__":
    main()