from datetime import datetime
import os
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {"owner": "airflow", "retries": 0}

with DAG(
    dag_id="data_quality_validation",
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/15 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["quality","validation"],
) as dag:

    validate = BashOperator(
        task_id="run_expectations",
        bash_command="python /opt/airflow/scripts/run_gx_validation.py",
        env={"DATABASE_URL": os.environ.get("DATABASE_URL", "postgresql+psycopg2://hdqp:hdqp@host.docker.internal:5432/hdqp")}
    )

    validate
