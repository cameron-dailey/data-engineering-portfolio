import os
import logging
from typing import Iterable
from sqlalchemy import create_engine, text
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("etl_utils")

def get_engine():
    """
    Create a SQLAlchemy engine from env var DATABASE_URL.
    Defaults to a local SQLite file for zero-config runs.
    """
    url = os.getenv("DATABASE_URL", "sqlite:///data/warehouse.db")
    if url.startswith("sqlite:///"):
        db_path = url.replace("sqlite:///", "")
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
    engine = create_engine(url, future=True)
    logger.info(f"Using database: {url}")
    return engine

def upsert_dataframe(df: pd.DataFrame, table: str, engine, unique_cols: Iterable[str]):
    if df.empty:
        logger.info(f"No rows to upsert for {table}")
        return
    keys = list(unique_cols)
    placeholders = " AND ".join([f"{k} = :{k}" for k in keys])
    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(text(f"DELETE FROM {table} WHERE {placeholders}"), {k: row[k] for k in keys})
        df.to_sql(table, conn, if_exists="append", index=False)

def normalize_columns(df: pd.DataFrame):
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df
