from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import subprocess
import pandas as pd

# Dossier de travail
DATA_DIR = "/opt/airflow/data"
GZ_FILE = os.path.join(DATA_DIR, "meteo.csv.gz")

def download_data():
    """Télécharge un fichier .csv.gz depuis une URL."""
    os.makedirs(DATA_DIR, exist_ok=True)
    url = "https://data.enedis.fr/api/explore/v2.1/catalog/datasets/fa-entrees/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B"  
    command = ["curl", "-L", "-o", GZ_FILE, url]
    result = subprocess.run(command, capture_output=True, text=True)

    if result.returncode == 0:
        print(f"Fichier téléchargé avec succès : {GZ_FILE}")
    else:
        raise Exception(f"Erreur lors du téléchargement : {result.stderr}")

def read_data():
    """Lit un fichier CSV compressé (.gz) et affiche un aperçu."""
    if not os.path.exists(GZ_FILE):
        raise FileNotFoundError(f"Le fichier .gz n'existe pas : {GZ_FILE}")

    try:
        df = pd.read_csv(GZ_FILE, compression='gzip', encoding='ISO-8859-1', delimiter=';')
        print("Aperçu des données :")
        print(df.head())
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier .gz : {e}")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 20),
    "retries": 0,
}

dag = DAG(
    "download_and_process_meteo_gz_csv_data",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)

download_task = PythonOperator(
    task_id="download_gz_csv",
    python_callable=download_data,
    dag=dag,
)

read_task = PythonOperator(
    task_id="read_gz_csv",
    python_callable=read_data,
    dag=dag,
)

download_task >> read_task

