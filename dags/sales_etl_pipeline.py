from airflow.decorators import dag, task
from datetime import datetime
from scripts.notify import notify_success, task_fail_alert
from scripts.extract import extract_sales_data
from scripts.transform import transform_sales_data
from scripts.load import load_sales_summary

@dag(
    dag_id="sales_etl_pipeline",
    description="Daily ETL pipeline for Superstore dataset (CSV → Parquet → Aggregation)",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "airflow", "project4", "data-engineering"],
    default_args={
        "retries": 0,
        "on_failure_callback": task_fail_alert,
    },
    on_success_callback=notify_success,
)
def sales_etl_pipeline():
    """
    End-to-end ETL pipeline:
    1. Extract raw CSV data
    2. Transform and aggregate sales data
    3. Load summarized results
    """

    @task(task_id="extract_sales_data")
    def extract():
        """Extract raw sales data from CSV and save as parquet file."""
        extract_sales_data()

    @task(task_id="transform_sales_data")
    def transform():
        """Transform sales data and generate aggregated summary."""
        transform_sales_data()

    @task(task_id="load_sales_summary")
    def load():
        """Load aggregated sales summary into final storage."""
        load_sales_summary()

    extract() >> transform() >> load()


dag = sales_etl_pipeline()