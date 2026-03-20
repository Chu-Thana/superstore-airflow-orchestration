from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.extract import extract_sales_data
from scripts.transform import transform_sales_data
from scripts.load import load_sales_summary
import sys
sys.path.append("/opt/airflow")


with DAG(
    dag_id="sales_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_sales_data",
        python_callable=extract_sales_data,
    )

    transform_task = PythonOperator(
        task_id="transform_sales_data",
        python_callable=transform_sales_data,
    )

    load_task = PythonOperator(
        task_id="load_sales_summary",
        python_callable=load_sales_summary,
    )

    extract_task >> transform_task >> load_task