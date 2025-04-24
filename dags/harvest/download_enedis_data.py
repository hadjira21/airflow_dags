from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import requests
import pandas as pd

# Constantes
DATA_DIR = "/opt/airflow/data/enedis"
CSV_PATH = os.path.join(DATA_DIR, "consommation_commune.csv")
DATASET_ID = "consommation-elec-gaz-commune"  # ID du dataset sur data.enedis.fr
API_URL = f"https://data.enedis.fr/api/records/1.0/search/?dataset={DATASET_ID}&rows=10000"

# Fonctions
def download_enedis_data():
    os.makedirs(DATA_DIR, exist_ok=True)
    response = requests.get(API_URL)

    if response.status_code == 200:
        data = response.json()
        records = [record["fields"] for record in data["records"]]
        df = pd.DataFrame.from_records(records)
        df.to_csv(CSV_PATH, index=False)
        print(f"Fichier CSV sauvegardé à : {CSV_PATH}")
    else:
        raise Exception(f"Erreur API Enedis : {response.status_code} - {response.text}")

def read_csv_data():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"Le fichier CSV est introuvable : {CSV_PATH}")
    
    df = pd.read_csv(CSV_PATH)
    print("Aperçu des données :")
    print(df.head())

# DAG Definition
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 20),
    "retries": 0,
}

dag = DAG(
    "download_enedis_data",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)

download_task = PythonOperator(
    task_id="download_enedis_data",
    python_callable=download_enedis_data,
    dag=dag,
)

read_task = PythonOperator(
    task_id="read_enedis_data",
    python_callable=read_csv_data,
    dag=dag,
)

download_task >> read_task
