import os, sys, yaml, pandas as pd
from sqlalchemy import create_engine

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://hdqp:hdqp@localhost:5432/hdqp")

def fail(msg, failures):
    failures.append(msg)
    print(f"[FAIL] {msg}")

def pass_(msg):
    print(f"[PASS] {msg}")

def validate():
    failures = []
    engine = create_engine(DATABASE_URL, future=True)
    suite = yaml.safe_load(open(os.path.join(os.path.dirname(__file__), "..", "expectations", "patient_expectations.yaml"), "r", encoding="utf-8"))

    events = pd.read_sql("SELECT * FROM warehouse.events", con=engine)
    print(f"Loaded {len(events)} rows from warehouse.events")

    for exp in suite.get("expectations", []):
        if "expect_table_row_count_to_be_greater_than" in exp:
            v = exp["expect_table_row_count_to_be_greater_than"]["value"]
            (pass_ if len(events) > v else fail)(f"Row count {'>' if len(events) > v else '<='} {v}", failures)

        if "expect_column_to_exist" in exp:
            col = exp["expect_column_to_exist"]["column"]
            (pass_ if col in events.columns else fail)(f"Column exists: {col}" if col in events.columns else f"Missing column: {col}", failures)

        if "expect_column_values_to_not_be_null" in exp and "column" in exp["expect_column_values_to_not_be_null"]:
            col = exp["expect_column_values_to_not_be_null"]["column"]
            (pass_ if not events[col].isna().any() else fail)(f"No nulls in {col}" if not events[col].isna().any() else f"Nulls found in {col}", failures)

        if "expect_column_values_to_be_in_set" in exp:
            col = exp["expect_column_values_to_be_in_set"]["column"]
            allowed = set(exp["expect_column_values_to_be_in_set"]["value_set"])
            bad = set(events[col].dropna().unique()) - allowed
            (pass_ if not bad else fail)(f"All values of {col} in set" if not bad else f"Unexpected values in {col}: {bad}", failures)

        if "expect_column_pair_values_A_to_be_less_than_B" in exp:
            table = exp["expect_column_pair_values_A_to_be_less_than_B"].get("table")
            colA = exp["expect_column_pair_values_A_to_be_less_than_B"]["column_A"]
            colB = exp["expect_column_pair_values_A_to_be_less_than_B"]["column_B"]
            df = pd.read_sql(f"SELECT * FROM {table}" if table else "SELECT * FROM warehouse.events", con=engine)
            bad = ((df[colA].notna()) & (df[colB].notna()) & (df[colA] >= df[colB])).any()
            (pass_ if not bad else fail)(f"{colA} < {colB} in {table or 'events'}" if not bad else f"Some rows have {colA} >= {colB} in {table or 'events'}", failures)

    if failures:
        print("\nSUMMARY: FAIL")
        for m in failures: print(" -", m)
        sys.exit(1)
    else:
        print("\nSUMMARY: PASS")
        sys.exit(0)

if __name__ == "__main__":
    validate()
