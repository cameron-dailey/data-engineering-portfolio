import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def test_kpi_daily_exists():
    load_dotenv()
    engine = create_engine(os.getenv("DATABASE_URL", "sqlite:///db/steelcity.db"), future=True)
    df = pd.read_sql_query(text("SELECT name FROM sqlite_master WHERE type='table' AND name='kpi_daily'"), engine)
    assert len(df) == 1, "kpi_daily table should exist after running build_features.py"
