from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import os
from airflow import DAG
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from datetime import datetime

# Paramètres DBT Cloud
DBT_CLOUD_ACCOUNT_ID = 70471823469539
DBT_CLOUD_JOB_ID = 70471837242727

dag = DAG(
    dag_id='trigger_dbt_cloud_job',
    start_date=datetime(2025, 6, 16),
    schedule_interval='@daily',
    catchup=False,
    tags=['dbt', 'dbt_cloud'],)
run_dbt_job = DbtCloudRunJobOperator(
        task_id= DBT_CLOUD_JOB_ID',
        job_id=<ID_DE_TON_JOB_DBT_CLOUD>,  # récupère dans DBT Cloud UI
        poll_interval=10,
        timeout=600,
        wait_for_termination=True,
        deferrable=False,
        dag=dag ) 
        # connection_id='dbt_cloud_default'  # optionnel si tu as nommé ta connexion par défaut
   
run_dbt_job
