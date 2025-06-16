from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
import os
import requests
import pandas as pd
import unidecode

# Dossiers
DATA_DIR = "/opt/airflow/data/climat"
CSV_FILE = os.path.join(DATA_DIR, "climat.csv")

CSV_URL = "https://www.data.gouv.fr/fr/datasets/r/eb4d0600-e90b-4517-a429-599ed13dbae0"  # Exemple pour un CSV

def download_csv():
    os.makedirs(DATA_DIR, exist_ok=True)
    response = requests.get(CSV_URL)
    if response.status_code == 200:
        with open(CSV_FILE, "wb") as f:
            f.write(response.content)
        print("Téléchargement CSV réussi.")
    else:
        raise Exception(f"Erreur de téléchargement : {response.status_code}")

def transform_file():
    df = pd.read_csv(CSV_FILE, delimiter=';', encoding='utf-8', compression='infer', low_memory=False)

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
    "load_climat_csv",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
)

download = PythonOperator(task_id="download_csv", python_callable=download_csv, dag=dag)
transform = PythonOperator(task_id="transform_file", python_callable=transform_file, dag=dag)
load = PythonOperator(task_id="load_to_snowflake", python_callable=load_to_snowflake, dag=dag)

download >> transform >> load
