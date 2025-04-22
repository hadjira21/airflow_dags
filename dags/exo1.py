from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import subprocess
import zipfile
import pandas as pd 

# Définition du dossier de stockage
DATA_DIR = "/opt/airflow/data"
ZIP_FILE = os.path.join(DATA_DIR, "eCO2mix_RTE_En-cours-TR.zip")
EXTRACTED_DIR = os.path.join(DATA_DIR, "eCO2mix_RTE_En-cours-TR")
CSV_DIR = os.path.join(DATA_DIR, "csv_files")  # Dossier pour les fichiers CSV

def download_data():
    """Télécharge le fichier ZIP depuis Kaggle."""
    os.makedirs(DATA_DIR, exist_ok=True)  # Crée le dossier s'il n'existe pas
    url = "https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_En-cours-TR.zip"
    command = ["curl", "-L", "-o", ZIP_FILE, url]
    result = subprocess.run(command, capture_output=True, text=True)

    if result.returncode == 0:
        print(f"Fichier téléchargé avec succès : {ZIP_FILE}")
    else:
        raise Exception(f"Erreur lors du téléchargement : {result.stderr}")

def unzip_data():
    """Décompresse le fichier ZIP."""
    if not os.path.exists(ZIP_FILE):
        raise FileNotFoundError(f"Le fichier ZIP n'existe pas : {ZIP_FILE}")

    os.makedirs(EXTRACTED_DIR, exist_ok=True)  # Crée le dossier d'extraction
    with zipfile.ZipFile(ZIP_FILE, 'r') as zip_ref:
        zip_ref.extractall(EXTRACTED_DIR)
    print(f"Fichiers extraits dans : {EXTRACTED_DIR}")

def rename_xls_to_csv(xls_path, csv_path):
    """Renomme le fichier .xls en .csv."""
    try:
        if os.path.exists(xls_path):
            os.rename(xls_path, csv_path)
            print(f"Fichier renommé de {xls_path} à {csv_path}")
        else:
            raise FileNotFoundError(f"Le fichier {xls_path} n'a pas été trouvé.")
    except Exception as e:
        print(f"Une erreur est survenue : {e}")

def read_data():
    """Lit et affiche un aperçu des données."""
    csv_path = '/opt/airflow/data/eCO2mix_RTE_En-cours-TR/eCO2mix_RTE_En-cours-TR.csv'  # Chemin du fichier CSV renommé
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Aucun fichier CSV trouvé : {csv_path}")

    try:
        # Lire le fichier CSV avec un encodage alternatif
        df = pd.read_csv(csv_path, encoding='ISO-8859-1', delimiter=';')
        # Utilisation de ISO-8859-1 pour les caractères spéciaux
        print("Aperçu des données :")
        print(df.head())
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier CSV : {e}")


# Définition du DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 20),
    "retries": 0,
}

dag = DAG(
    "download_and_process_data_eco2mix",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)

# Définition des tâches
download_task = PythonOperator(
    task_id="download_data",
    python_callable=download_data,
    dag=dag,
)

unzip_task = PythonOperator(
    task_id="unzip_data",
    python_callable=unzip_data,
    dag=dag,
)

# Définir les chemins des fichiers
xls_file_path = '/opt/airflow/data/eCO2mix_RTE_En-cours-TR/eCO2mix_RTE_En-cours-TR.xls'
csv_file_path = '/opt/airflow/data/eCO2mix_RTE_En-cours-TR/eCO2mix_RTE_En-cours-TR.csv'

# Nouvelle tâche pour renommer le fichier .xls en .csv
rename_task = PythonOperator(
    task_id='rename_xls_to_csv',
    python_callable=rename_xls_to_csv,
    op_args=[xls_file_path, csv_file_path],  # Arguments passés à la fonction
    dag=dag,
)

read_task = PythonOperator(
    task_id="read_data",
    python_callable=read_data,
    dag=dag,
)

# Définir l'ordre d'exécution
download_task >> unzip_task >> rename_task >> read_task
