import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

def get_engine():
    url = os.getenv("DATABASE_URL", "sqlite:///db/steelcity.db")
    if url.startswith("sqlite") and url.endswith("steelcity.db"):
        # Ensure parent dir exists
        os.makedirs("db", exist_ok=True)
    engine = create_engine(url, future=True)
    return engine

def upsert_dataframe(df, table_name, engine, if_exists="append", dtype=None):
    # Simple append (SQLite doesn't have native upsert w/ SQLAlchemy pandas.to_sql)
    df.to_sql(table_name, engine, if_exists=if_exists, index=False, dtype=dtype)

def run_sql(sql_text, engine):
    with engine.begin() as conn:
        conn.execute(text(sql_text))
