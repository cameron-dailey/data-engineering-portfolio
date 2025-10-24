from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "steelcity",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="steelcity_ops_etl",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["steelcity", "analytics"],
) as dag:

    ingest_bookings = BashOperator(
        task_id="ingest_bookings",
        bash_command="python pipelines/ingest/etl_bookings.py",
    )
    ingest_weather = BashOperator(
        task_id="ingest_weather",
        bash_command="python pipelines/ingest/etl_weather.py",
    )
    ingest_maintenance = BashOperator(
        task_id="ingest_maintenance",
        bash_command="python pipelines/ingest/etl_maintenance.py",
    )
    build_features = BashOperator(
        task_id="build_features",
        bash_command="python pipelines/transform/build_features.py",
    )

    [ingest_bookings, ingest_weather, ingest_maintenance] >> build_features
