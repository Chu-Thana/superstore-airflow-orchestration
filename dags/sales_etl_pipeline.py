from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.extract import extract_sales_data
from scripts.transform import transform_sales_data
from scripts.load import load_sales_summary


dag = DAG(
    dag_id="sales_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
)


extract_task = PythonOperator(
    task_id="extract_sales_data",
    python_callable=extract_sales_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_sales_data",
    python_callable=transform_sales_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_sales_summary",
    python_callable=load_sales_summary,
    dag=dag,
)

extract_task >> transform_task >> load_task