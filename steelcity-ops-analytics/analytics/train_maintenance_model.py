import pandas as pd
from sqlalchemy import text
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import joblib
from common.etl_utils import get_engine

def main():
    engine = get_engine()

    # -----------------------------------------------
    # Build a labeled dataset:
    # Label = 1 if a maintenance event occurs within 7 days after a given feature day
    # -----------------------------------------------
    query = """
    WITH features AS (
        SELECT * FROM features_usage_weather
    ),
    maint AS (
        SELECT jet_ski_id, date AS maint_date
        FROM maintenance
    )
    SELECT 
        f.jet_ski_id,
        f.day,
        f.hours_used_day,
        f.avg_temp_c,
        f.avg_precip_mm,
        f.avg_wind_kmh,
        CASE 
            WHEN EXISTS (
                SELECT 1 
                FROM maint m
                WHERE m.jet_ski_id = f.jet_ski_id
                  AND m.maint_date > f.day
                  AND m.maint_date <= f.day + INTERVAL '7 days'
            ) THEN 1 
            ELSE 0 
        END AS needs_maintenance_7d
    FROM features f;
    """

    # -----------------------------------------------
    # Read data from database
    # -----------------------------------------------
    df = pd.read_sql_query(text(query), engine)
    df = df.fillna(0)

    if df.empty:
        print("No data returned from features_usage_weather. Run your transform pipeline first.")
        return

    # -----------------------------------------------
    # Train-test split and model training
    # -----------------------------------------------
    X = df[["hours_used_day", "avg_temp_c", "avg_precip_mm", "avg_wind_kmh"]]
    y = df["needs_maintenance_7d"]

    if len(y.unique()) < 2:
        print("Not enough class variation in the dataset to train the model.")
        return

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.25, random_state=42, stratify=y
    )

    clf = RandomForestClassifier(n_estimators=200, random_state=42)
    clf.fit(X_train, y_train)

    # -----------------------------------------------
    # Evaluate and save model
    # -----------------------------------------------
    preds = clf.predict(X_test)
    print(classification_report(y_test, preds))

    joblib.dump(clf, "models/maintenance_rf.pkl")
    print("✅ Model trained and saved to models/maintenance_rf.pkl")

if __name__ == "__main__":
    main()