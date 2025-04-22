from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_world():
    print("Hello, Airflow!")

with DAG(
    dag_id='hello_world_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['test'],
) as dag:
    task = PythonOperator(
        task_id='say_hello',
        python_callable=hello_world
    )
