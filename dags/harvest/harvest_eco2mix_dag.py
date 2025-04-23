from airflow import DAG
from datetime import datetime
from operators.eco2mix_operator import Eco2mixOperator

default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='harvest_eco2mix_dag',
    default_args=default_args,
    schedule_interval=None,  # Déclenchement manuel
    catchup=False,
    tags=['eco2mix']
) as dag:

    download_data = Eco2mixOperator(
        task_id='download_eco2mix_data',
        url="https://eco2mix.rte-france.com/download/eco2mix-national-tr.csv.zip", 
        output_path='/opt/airflow/data/eco2mix',
        start_date='2023-01-01',
        end_date='2023-12-31',
    )

    download_data
