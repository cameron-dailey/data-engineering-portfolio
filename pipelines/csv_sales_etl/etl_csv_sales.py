import pandas as pd
from pathlib import Path
from common.etl_utils import get_engine, normalize_columns, upsert_dataframe

def extract(path: str) -> pd.DataFrame:
    return pd.read_csv(path)

def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_columns(df)
    df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce")
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0).astype(int)
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["total_amount"] = df["qty"] * df["unit_price"]
    return df[["sale_id", "sale_date", "product", "qty", "unit_price", "total_amount"]]

def load(df: pd.DataFrame):
    engine = get_engine()
    upsert_dataframe(df, "fact_sales", engine, unique_cols=["sale_id"])

if __name__ == "__main__":
    data_path = Path(__file__).parent / "data" / "sales.csv"
    df = extract(str(data_path))
    df_t = transform(df)
    load(df_t)
    print(f"Loaded {len(df_t)} rows into fact_sales")
