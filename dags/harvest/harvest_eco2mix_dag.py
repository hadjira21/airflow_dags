from airflow import DAG
from datetime import datetime
from operators.Eco2mixOperator import Eco2mixOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    dag_id="harvest_eco2mix_dag",
    default_args=default_args,
    description="DAG de récolte des données eco2mix",
    schedule_interval="@daily",
    catchup=False
) as dag:

    download_eco2mix = Eco2mixOperator(
        task_id="download_eco2mix_data",
        url="https://eco2mix.rte-france.com/curves/download/eco2mix-national-tr.csv",
        output_path="/opt/airflow/data/eco2mix/eco2mix-national-tr.csv"
    )

    download_eco2mix
