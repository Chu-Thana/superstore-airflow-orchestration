from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="project1_etl_runner",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["project1", "etl"],
) as dag:

    check_source = BashOperator(
        task_id="check_source",
        bash_command="echo 'Checking ETL source...'",
    )

    run_etl = BashOperator(
        task_id="run_etl",
        bash_command="python /opt/airflow/scripts/run_etl.py",
    )

    validate_output = BashOperator(
        task_id="validate_output",
        bash_command="cat /opt/airflow/data/etl_output.txt",
    )

    check_source >> run_etl >> validate_output