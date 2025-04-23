from airflow import DAG
from datetime import datetime
from airflow.utils.dates import days_ago
from dags.hooks.eco2mix_operator import Eco2mixDownloadOperator

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1)
}

with DAG(
    dag_id="eco2mix_data_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Téléchargement des données Eco2mix via opérateur personnalisé"
) as dag:

    download_data = Eco2mixDownloadOperator(
        task_id="download_eco2mix_data",
        output_path="/opt/airflow/data/eco2mix_data.csv",
        start_date="2023-01-01",
        end_date="2023-12-31"
    )

    download_data
