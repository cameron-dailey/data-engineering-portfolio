from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
import sys
from pathlib import Path

BASE=Path(__file__).resolve().parents[1]
SRC=BASE/"src"
sys.path.append(str(SRC))

from simulate_data import main as sim
from ingest import ingest
from transform import transform
from run_lift_model import estimate_incrementality

default_args={"owner":"cameron","retries":1,"retry_delay":timedelta(minutes=5)}

with DAG("marketing_incrementality",default_args=default_args,start_date=datetime(2023,1,1),schedule_interval="@daily",catchup=False):
    t1=PythonOperator(task_id="simulate",python_callable=sim)
    t2=PythonOperator(task_id="ingest",python_callable=ingest)
    t3=PythonOperator(task_id="transform",python_callable=transform)
    t4=PythonOperator(task_id="lift",python_callable=estimate_incrementality)
    t1>>t2>>t3>>t4
