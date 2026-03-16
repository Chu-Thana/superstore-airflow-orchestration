from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


# -----------------------------
# Tasks
# -----------------------------

def extract_data():
    print("Extracting sales data...")


def transform_data():
    print("Transforming sales data...")


def load_data():
    print("Loading data to warehouse...")


# -----------------------------
# DAG
# -----------------------------

with DAG(
    dag_id="sales_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_sales_data",
        python_callable=extract_data
    )

    transform_task = PythonOperator(
        task_id="transform_sales_data",
        python_callable=transform_data
    )

    load_task = PythonOperator(
        task_id="load_sales_summary",
        python_callable=load_data
    )

    extract_task >> transform_task >> load_task