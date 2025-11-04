from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os, sys
from pathlib import Path

PORTFOLIO_DIR = os.environ.get("PORTFOLIO_DIR", "/opt/airflow/dags/portfolio")
sys.path.append(str(Path(PORTFOLIO_DIR)))

def run_csv_sales():
    import pipelines.csv_sales_etl.etl_csv_sales as job
    job.__main__()

def run_excel_customers():
    import pipelines.excel_customers_etl.etl_excel_customers as job
    job.__main__()

def run_api_orders():
    import pipelines.api_orders_etl.etl_api_orders as job
    job.__main__()

default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="general_etl_dag",
    default_args=default_args,
    description="Run multiple small ETL jobs: CSV, Excel, JSON",
    start_date=datetime(2025, 9, 1),
    schedule="0 2 * * *",
    catchup=False,
    tags=["etl", "portfolio"],
) as dag:

    t1 = PythonOperator(task_id="csv_sales", python_callable=run_csv_sales)
    t2 = PythonOperator(task_id="excel_customers", python_callable=run_excel_customers)
    t3 = PythonOperator(task_id="api_orders", python_callable=run_api_orders)

    [t1, t2] >> t3
