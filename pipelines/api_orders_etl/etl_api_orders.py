import json
import pandas as pd
from pathlib import Path
from common.etl_utils import get_engine, normalize_columns, upsert_dataframe

def extract(path: str) -> pd.DataFrame:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return pd.DataFrame(data)

def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_columns(df)
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["order_total"] = pd.to_numeric(df["order_total"], errors="coerce")
    return df[["order_id", "customer_id", "status", "order_total", "created_at"]]

def load(df: pd.DataFrame):
    engine = get_engine()
    upsert_dataframe(df, "fact_orders", engine, unique_cols=["order_id"])

if __name__ == "__main__":
    data_path = Path(__file__).parent / "data" / "api_orders.json"
    df = extract(str(data_path))
    df_t = transform(df)
    load(df_t)
    print(f"Loaded {len(df_t)} rows into fact_orders")
