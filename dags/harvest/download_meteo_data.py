from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import subprocess
import pandas as pd

# Dossier de travail
DATA_DIR = "/opt/airflow/data"
GZ_FILE = os.path.join(DATA_DIR, "meteo.csv.gz")
CSV_FILE = os.path.join(DATA_DIR, "meteo.csv")  # Fichier décompressé
def download_data():
    """Télécharge un fichier .csv.gz depuis une URL."""
    os.makedirs(DATA_DIR, exist_ok=True)
    url = "https://www.data.gouv.fr/fr/datasets/r/c1265c02-3a8e-4a28-961e-26b2fd704fe8"  # Remplace avec ton URL réelle
    command = ["curl", "-L", "-o", GZ_FILE, url]
    result = subprocess.run(command, capture_output=True, text=True)

    if result.returncode == 0:
        print(f"Fichier téléchargé avec succès : {GZ_FILE}")
    else:
        raise Exception(f"Erreur lors du téléchargement : {result.stderr}")


def decompress_file():
    """Décompresse le fichier .csv.gz."""
    if not os.path.exists(GZ_FILE):
        raise FileNotFoundError(f"Le fichier .gz n'existe pas : {GZ_FILE}")

    try:
        with gzip.open(GZ_FILE, 'rb') as f_in:
            with open(CSV_FILE, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print(f"Fichier décompressé avec succès : {CSV_FILE}")
    except Exception as e:
        raise Exception(f"Erreur lors de la décompression du fichier .gz : {e}")




def read_data():
    """Lit le fichier CSV décompressé et affiche un aperçu."""
    if not os.path.exists(CSV_FILE):
        raise FileNotFoundError(f"Le fichier CSV n'existe pas : {CSV_FILE}")

    try:
        df = pd.read_csv(CSV_FILE, encoding='ISO-8859-1', delimiter=';')
        print("Aperçu des données :")
        print(df.head())
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier CSV : {e}")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 20),
    "retries": 0,
}

dag = DAG(
    "download_meteo_gz_csv_data",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)

download_task = PythonOperator(
    task_id="download_gz_csv",
    python_callable=download_data,
    dag=dag,
)
decompress_task = PythonOperator(
    task_id="decompress_gz_csv",
    python_callable=decompress_file,
    dag=dag,
)

read_task = PythonOperator(
    task_id="read_gz_csv",
    python_callable=read_data,
    dag=dag,
)

download_task>>decompress_task>>read_task
