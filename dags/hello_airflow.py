from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


def say_hello():
    print("Hello Airflow!")


with DAG(
    dag_id="hello_airflow_pipeline",
    start_date=datetime(2026, 3, 15),
    schedule="@daily",
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="say_hello_task",
        python_callable=say_hello,
    )