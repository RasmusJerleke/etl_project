from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG("etl", start_date=datetime(2021,1,1),
    schedule_interval="* * * * *", catchup=False) as dag:

    test = BashOperator(
        task_id='test',
        bash_command='echo "Hej"'
    )
