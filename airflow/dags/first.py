from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# -------------------------------
# DAG аргументы
# -------------------------------
default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 3, 5),
    "retries": 1,
}

# -------------------------------
# DAG
# -------------------------------
with DAG(
    dag_id="xcom_example_dag",
    default_args=default_args,
    schedule=None,  # ручной запуск
    catchup=False,
    tags=["example", "xcom"],
) as dag:

    # -------------------------------
    # Task 1: Генерация данных
    # -------------------------------
    def generate_number(ti):
        value = 42
        print(f"Generated value: {value}")
        # Передаем значение через XCom
        ti.xcom_push(key="my_number", value=value)

    task_generate = PythonOperator(
        task_id="generate_number",
        python_callable=generate_number,
    )

    # -------------------------------
    # Task 2: Используем данные из XCom
    # -------------------------------
    def use_number(ti):
        # Получаем значение из XCom
        value = ti.xcom_pull(key="my_number", task_ids="generate_number")
        print(f"Received value from XCom: {value}")

    task_use = PythonOperator(
        task_id="use_number",
        python_callable=use_number,
    )

    # -------------------------------
    # Зависимости
    # -------------------------------
    task_generate >> task_use