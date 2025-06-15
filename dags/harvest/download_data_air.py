from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import os

# Constantes
API_URL = "https://api.atmosud.org/iqa2021/commune/indices/journalier"
INSEE_CODE = "13055"  # Marseille
DATE_RANGE = "2021-01-04,2021-01-06"
OUTPUT_DIR = "/opt/airflow/data/atmosud/"
OUTPUT_FILE = f"{OUTPUT_DIR}indices_13055_2021-01-04_2021-01-06.csv"

def fetch_atmosud_data():
    url = f"{API_URL}?dates_echeances={DATE_RANGE}&insee={INSEE_CODE}"
    
    response = requests.get(url)
    response.raise_for_status()
    
    data = response.json()
    records = data.get("data", [])
    
    if not records:
        print("Aucun enregistrement trouvé.")
        return
    
    df = pd.json_normalize(records)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Données sauvegardées dans : {OUTPUT_FILE}")

default_args = {
    'start_date': datetime(2021, 1, 7),
    'catchup': False,
}

with DAG(
    dag_id="atmosud_single_commune_dag",
    schedule_interval=None,  # Exécution manuelle
    default_args=default_args,
    tags=["atmosud", "pollution", "api"],
) as dag:

    download_task = PythonOperator(
        task_id="download_atmosud_data",
        python_callable=fetch_atmosud_data
    )

    download_task
