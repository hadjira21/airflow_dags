from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import subprocess
import pandas as pd

# Dossier de destination
DATA_DIR = "/opt/airflow/data"
CSV_FILE = os.path.join(DATA_DIR, "electrique_commune.csv")

def download_data():
    """Télécharge le fichier CSV depuis l'URL fournie."""
    os.makedirs(DATA_DIR, exist_ok=True)
    
    
    url = "https://data.enedis.fr/api/explore/v2.1/catalog/datasets/production-electrique-par-filiere-a-la-maille-commune"
    command = ["curl", "-L", "-o", CSV_FILE, url]
    result = subprocess.run(command, capture_output=True, text=True)

    if result.returncode == 0:
        print(f"Fichier téléchargé avec succès : {CSV_FILE}")
    else:
        raise Exception(f"Erreur lors du téléchargement : {result.stderr}")

def read_data():
    if not os.path.exists(CSV_FILE):
        raise FileNotFoundError(f"Le fichier CSV n'existe pas : {CSV_FILE}")
    try:
        df = pd.read_csv(CSV_FILE, encoding='ISO-8859-1', delimiter=';')
        print("Aperçu des données :")
        print(df.head())
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier CSV : {e}")

default_args = {  "owner": "airflow", "start_date": datetime(2025, 3, 20), "retries": 0,}

dag = DAG( "download_and_process_electrique_data", default_args=default_args, schedule_interval="@daily", catchup=False,)
download_task = PythonOperator( task_id="download_csv_data", python_callable=download_data, dag=dag, )
read_task = PythonOperator(task_id="read_csv_data", python_callable=read_data, dag=dag,)
download_task >> read_task
