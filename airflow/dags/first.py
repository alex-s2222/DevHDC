from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def hello_world():
    print("Hello from Airflow DAG!")
    return "success"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="test_dag",
    default_args=default_args,
    description="Simple test DAG",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["test"],
) as dag:

    task1 = PythonOperator(
        task_id="hello_task",
        python_callable=hello_world,
    )

    task1