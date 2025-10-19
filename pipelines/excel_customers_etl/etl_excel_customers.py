import pandas as pd
from pathlib import Path
from common.etl_utils import get_engine, normalize_columns, upsert_dataframe

def extract(path: str) -> pd.DataFrame:
    return pd.read_excel(path)

def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_columns(df)
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    return df[["customer_id", "customer_name", "email", "created_at"]]

def load(df: pd.DataFrame):
    engine = get_engine()
    upsert_dataframe(df, "dim_customers", engine, unique_cols=["customer_id"])

if __name__ == "__main__":
    data_path = Path(__file__).parent / "data" / "customers.xlsx"
    df = extract(str(data_path))
    df_t = transform(df)
    load(df_t)
    print(f"Loaded {len(df_t)} rows into dim_customers")
