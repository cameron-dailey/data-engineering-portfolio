from datetime import datetime
import os
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {"owner": "airflow", "retries": 0}

with DAG(
    dag_id="healthcare_flow_etl",
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/10 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["etl","healthcare"],
) as dag:

    transform = BashOperator(
        task_id="transform_patient_data",
        bash_command="python /opt/airflow/scripts/transform_patient_data.py",
        env={"DATABASE_URL": os.environ.get("DATABASE_URL", "postgresql+psycopg2://hdqp:hdqp@host.docker.internal:5432/hdqp")}
    )

    transform
