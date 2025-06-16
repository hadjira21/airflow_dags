from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
import os
import requests
import zipfile
import pandas as pd
import unidecode

# Dossiers
DATA_DIR = "/opt/airflow/data/climat"
ZIP_FILE = os.path.join(DATA_DIR, "climat.zip")
EXTRACTED_DIR = os.path.join(DATA_DIR, "extracted")
CSV_FILE = os.path.join(DATA_DIR, "climat.csv")

# URL de la ressource ZIP la plus récente
RESOURCE_URL = "https://www.data.gouv.fr/fr/datasets/r/eb4d0600-e90b-4517-a429-599ed13dbae0"  # à mettre à jour si besoin

def download_zip():
    os.makedirs(DATA_DIR, exist_ok=True)
    response = requests.get(RESOURCE_URL)
    if response.status_code == 200:
        with open(ZIP_FILE, "wb") as f:
            f.write(response.content)
        print("Téléchargement ZIP réussi.")
    else:
        raise Exception(f"Erreur de téléchargement : {response.status_code}")

def unzip_file():
    if not os.path.exists(ZIP_FILE):
        raise FileNotFoundError("Fichier ZIP non trouvé.")
    os.makedirs(EXTRACTED_DIR, exist_ok=True)
    with zipfile.ZipFile(ZIP_FILE, 'r') as zip_ref:
        zip_ref.extractall(EXTRACTED_DIR)
    print("Décompression réussie.")

def transform_file():
    file_list = [f for f in os.listdir(EXTRACTED_DIR) if f.endswith('.csv')]
    if not file_list:
        raise FileNotFoundError("Aucun fichier CSV trouvé.")
    csv_path = os.path.join(EXTRACTED_DIR, file_list[0])
    df = pd.read_csv(csv_path, delimiter=';', encoding='utf-8', low_memory=False)

    df.columns = [unidecode.unidecode(col.strip().replace(" ", "_")) for col in df.columns]
    df["AAAAMMJJ"] = pd.to_datetime(df["AAAAMMJJ"], format="%Y%m%d", errors="coerce")

    float_cols = [col for col in df.columns if col not in ["NUM_POSTE", "NOM_USUEL", "AAAAMMJJ"] and not col.startswith("Q")]
    for col in float_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df.to_csv(CSV_FILE, index=False, sep=';', encoding='utf-8')
    print("Transformation complète.")

def load_to_snowflake():
    conn_params = {
        'user': 'HADJIRA25',
        'password': '42XCDpmzwMKxRww',
        'account': 'TRMGRRV-JN45028',
        'warehouse': 'INGESTION_WH',
        'database': 'BRONZE',
        'schema': "METEO"
    }
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn', **conn_params)

    hook.run("USE DATABASE BRONZE;")
    hook.run("USE SCHEMA METEO;")

    hook.run("""CREATE OR REPLACE TABLE climat_data (
        NUM_POSTE STRING, NOM_USUEL STRING, LAT FLOAT, LON FLOAT, ALTI INT, AAAAMMJJ DATE,
        RR FLOAT, QRR STRING, TN FLOAT, QTN STRING, HTN FLOAT, QHTN STRING,
        TX FLOAT, QTX STRING, HTX FLOAT, QHTX STRING, TM FLOAT, QTM STRING,
        TNX FLOAT, QTNX STRING, TAMPLI FLOAT, QTAMPLI STRING, TNSOL FLOAT, QTNSOL STRING,
        TN50 FLOAT, QTN50 STRING, DG FLOAT, QDG STRING, FFM FLOAT, QFFM STRING,
        FF2M FLOAT, QFF2M STRING, FXY FLOAT, QFXY STRING, DXY FLOAT, QDXY STRING,
        HXY FLOAT, QHXY STRING, FXI FLOAT, QFXI STRING, DXI FLOAT, QDXI STRING,
        HXI FLOAT, QHXI STRING, FXI2 FLOAT, QFXI2 STRING, DXI2 FLOAT, QDXI2 STRING,
        HXI2 FLOAT, QHXI2 STRING, FXI3S FLOAT, QFXI3S STRING, DXI3S FLOAT, QDXI3S STRING,
        HXI3S FLOAT, QHXI3S STRING, DRR FLOAT, QDRR STRING
    );""")

    put_command = f"PUT file://{CSV_FILE} @METEO_STAGE"
    hook.run(put_command)

    copy_sql = """
    COPY INTO climat_data
    FROM @METEO_STAGE/climat.csv
    FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1, FIELD_DELIMITER = ';', ENCODING = 'UTF8', REPLACE_INVALID_CHARACTERS = TRUE)
    FORCE = TRUE
    ON_ERROR = 'CONTINUE';
    """
    hook.run(copy_sql)
    print("Données chargées dans Snowflake.")

# DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 6, 1),
    "retries": 0
}

dag = DAG(
    "load_climat_data",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
)

download = PythonOperator(task_id="download_zip", python_callable=download_zip, dag=dag)
unzip = PythonOperator(task_id="unzip_file", python_callable=unzip_file, dag=dag)
transform = PythonOperator(task_id="transform_file", python_callable=transform_file, dag=dag)
load = PythonOperator(task_id="load_to_snowflake", python_callable=load_to_snowflake, dag=dag)

download >> unzip >> transform >> load
