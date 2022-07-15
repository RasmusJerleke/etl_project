from platform import python_branch
import sys, os
sys.path.insert(1, os.path.abspath(os.path.join(__file__ ,"../..")))
from app import Etl #extract, transform, visualize, load, use_all_coordinates, setup, clean
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

etl = Etl()

def prepare():
    global etl
    etl = Etl()
    etl.silent = True
    # etl.use_all_coordinates()
    etl.clean()
    etl.setup()

with DAG("etl", start_date=datetime(2021,1,1),
    schedule_interval="*/10 * * * *", catchup=False) as dag:

    setup_task = PythonOperator(
        task_id = 'setup_task',
        python_callable = prepare 
    )

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable = etl.extract
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable = etl.transform
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable = etl.load
    )

    visualize_task = PythonOperator(
        task_id='visualize_task',
        python_callable = etl.visualize
    )

    setup_task >> extract_task >> transform_task >> visualize_task >> load_task