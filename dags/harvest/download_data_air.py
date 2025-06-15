import requests
import pandas as pd
from io import StringIO
import os
from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook # type: ignore
from datetime import datetime
import subprocess

API_URL = "https://api.atmosud.org/iqa2021/commune/indices/journalier"
DATE_START = "2021-01-01"
DATE_END = "2021-01-07"
COMMUNES = ['13055', '06088', '34172']  

DATA_DIR = "/opt/airflow/data/atmosud"
os.makedirs(DATA_DIR, exist_ok=True)

def fetch_and_save_data(**kwargs):
    all_data = []
    for code_insee in COMMUNES:
        url = f"{API_URL}?dates_echeances={DATE_START},{DATE_END}&insee={code_insee}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            json_data = response.json()
            data = json_data.get("data", [])
            print(f"{len(data)} enregistrements pour INSEE {code_insee}")
            all_data.extend(data)
        except Exception as e:
            print(f"Erreur pour {code_insee} : {str(e)}")

    # Sauvegarder les données en CSV
    if all_data:
        df = pd.json_normalize(all_data)
        df.to_csv(os.path.join(DATA_DIR, "atmosud_communes.csv"), index=False, encoding="utf-8")
        print("Fichier sauvegardé avec succès.")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2021, 1, 1),
    "retries": 0,
}

with DAG(
    dag_id="fetch_atmosud_communes",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Télécharge les indices AtmoSud pour plusieurs communes",
) as dag:

    fetch_data_task = PythonOperator(
        task_id="fetch_atmosud_data",
        python_callable=fetch_and_save_data,
        provide_context=True,
    )
