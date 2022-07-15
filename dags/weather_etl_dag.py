from platform import python_branch
import sys, os
sys.path.insert(1, os.path.abspath(os.path.join(__file__ ,"../..")))
from app import Etl
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

etl = Etl()

def prepare():
    global etl
    etl = Etl()
    etl.silent = True
    etl.clean()
    etl.setup()

def extract():
    etl.extract(all=True)

with DAG("etl", start_date=datetime(2021,1,1),
    schedule_interval="0 0 * * *", catchup=False) as dag:

    setup_task = PythonOperator(
        task_id = 'setup_task',
        python_callable = prepare 
    )

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable = extract
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