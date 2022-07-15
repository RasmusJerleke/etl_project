import sys, os
sys.path.insert(1, os.path.abspath(os.path.join(__file__ ,"../..")))
from app import extract, transform, visualize, load, use_all_coordinates
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG("etl", start_date=datetime(2021,1,1),
    schedule_interval="* * * * *", catchup=False) as dag:

    extract = PythonOperator(
        task_id='extract',
        python_callable = extract
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable = transform
    )

    load = PythonOperator(
        task_id='load',
        python_callable = load
    )

    visualize = PythonOperator(
        task_id='visualize',
        python_callable = visualize
    )

    extract >> transform >> visualize >> load