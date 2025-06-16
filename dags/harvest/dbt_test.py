from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import os

# Paramètres DBT Cloud
DBT_CLOUD_ACCOUNT_ID = 70471823469539
DBT_CLOUD_JOB_ID = 70471837242727
DBT_CLOUD_API_TOKEN = "dbtc_44q-vb47_gAXkzqbgVjeUY-RZk6LX3RZrY-tXwhPHg6uc-rv5w"

DBT_TRIGGER_URL = f"https://cloud.getdbt.com/api/v2/accounts/{DBT_CLOUD_ACCOUNT_ID}/jobs/{DBT_CLOUD_JOB_ID}/run/"

def trigger_dbt_cloud_job():
    headers = {
        "Authorization": f"Token {DBT_CLOUD_API_TOKEN}",
        "Content-Type": "application/json"
    }

    response = requests.post(DBT_TRIGGER_URL, headers=headers)

    if response.status_code != 200:
        raise Exception(f"DBT Cloud job failed to trigger: {response.text}")
    else:
        run_id = response.json().get("data", {}).get("id")
        print(f"DBT Cloud job triggered successfully, run_id: {run_id}")

# DAG Airflow
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='trigger_dbt_silver_job',
    default_args=default_args,
    description='Déclenche un job DBT Cloud pour les modèles Silver',
    schedule_interval='0 6 * * *',  
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'silver']
)
trigger_dbt = PythonOperator(
        task_id='trigger_dbt_silver',
        python_callable=trigger_dbt_cloud_job,
        dag=dag    )
trigger_dbt
