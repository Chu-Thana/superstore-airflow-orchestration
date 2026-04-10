from datetime import datetime
import os
import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator


REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = int(os.getenv("REDSHIFT_PORT", "5439"))
REDSHIFT_DB = os.getenv("REDSHIFT_DB", "dev")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")

SQL_REBUILD_SALES_BY_REGION = """
DROP TABLE IF EXISTS mart_layer.sales_by_region;

CREATE TABLE mart_layer.sales_by_region AS
SELECT
    region,
    SUM(sales) AS total_sales
FROM raw_layer.sales_raw
GROUP BY region;
"""


def get_connection():
    return psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
    )


def check_redshift_connection():
    conn = None
    cursor = None
    try:
        conn = get_connection()
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute("SELECT 1;")
        print("[SUCCESS] Redshift connection is healthy.")
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def rebuild_sales_by_region():
    conn = None
    cursor = None
    try:
        conn = get_connection()
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(SQL_REBUILD_SALES_BY_REGION)
        print("[SUCCESS] mart_layer.sales_by_region rebuilt successfully.")
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


with DAG(
    dag_id="redshift_mart_pipeline",
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    catchup=False,
    tags=["airflow", "redshift", "warehouse"],
) as dag:

    check_redshift_connection_task = PythonOperator(
        task_id="check_redshift_connection",
        python_callable=check_redshift_connection,
    )

    rebuild_sales_by_region_task = PythonOperator(
        task_id="rebuild_sales_by_region",
        python_callable=rebuild_sales_by_region,
    )

    check_redshift_connection_task >> rebuild_sales_by_region_task