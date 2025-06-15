from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import subprocess
import pandas as pd
import os

DATA_DIR = "/opt/airflow/data"
CSV_FILE = os.path.join(DATA_DIR, "temperature_radiation.csv")

def download_data():
    """Télécharge le fichier CSV depuis l'URL fournie."""
    os.makedirs(DATA_DIR, exist_ok=True)
    url = "https://data.enedis.fr/api/explore/v2.1/catalog/datasets/donnees-de-temperature-et-de-pseudo-rayonnement/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B"
    command = ["curl", "-L", "-o", CSV_FILE, url]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Fichier téléchargé avec succès : {CSV_FILE}")
    else:
        raise Exception(f"Erreur lors du téléchargement : {result.stderr}")

def read_data():
    """Lit et affiche un aperçu des données téléchargées."""
    if not os.path.exists(CSV_FILE):
        raise FileNotFoundError(f"Le fichier CSV n'existe pas : {CSV_FILE}")

    try:
        df = pd.read_csv(CSV_FILE, encoding='ISO-8859-1', delimiter=';')
        print("Aperçu des données :")
        print(df.head())
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier CSV : {e}")

def upload_to_snowflake():
    conn_params = {'user': 'HADJIRA25', 'password' : '42XCDpmzwMKxRww', 'account': 'TRMGRRV-JN45028',
    'warehouse': 'COMPUTE_WH', 'database': 'BRONZE',  'schema': "ENEDIS" }
    snowflake_hook = SnowflakeHook( snowflake_conn_id='snowflake_conn', **conn_params)
    snowflake_hook.run(f"USE DATABASE {conn_params['database']}")
    snowflake_hook.run(f"USE SCHEMA {conn_params['schema']}")
    create_table_sql = """CREATE TABLE IF NOT EXISTS temperature_data (
        Horodate STRING, Temp_realisee_lissee_C FLOAT, Temp_normale_lissee_C FLOAT,
        Diff_Temp_Realisee_Normale_C FLOAT,  Pseudo_rayonnement FLOAT,
        Annee INT,Mois INT, Jour INT, Annee_Mois_Jour STRING); """
    snowflake_hook.run(create_table_sql)

default_args = { "owner": "airflow", "start_date": datetime(2025, 3, 20), "retries": 0,}
dag = DAG("download_and_process_temperature_data",description='DAG pour download & upload les données de température et de rayonnement dans Snowflake', default_args=default_args, schedule_interval="@daily", catchup=False,)
download_task = PythonOperator( task_id="download_csv_data", python_callable=download_data, dag=dag,)
read_task = PythonOperator(task_id="read_csv_data", python_callable=read_data, dag=dag,)
upload_task = PythonOperator(task_id='upload_to_snowflake', python_callable=upload_to_snowflake, dag=dag)
download_task >> read_task >> upload_task
