from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import zipfile
import os
import csv
import snowflake.connector

URL = "https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_En-cours-TR.zip"
LOCAL_ZIP_PATH = "/opt/airflow/data/eCO2mix_RTE_En-cours-TR.zip"
EXTRACTED_CSV_PATH = "/opt/airflow/data/eCO2mix_RTE_En-cours-TR/eCO2mix_RTE_En-cours-TR.csv"
SNOWFLAKE_TABLE = "BRONZE.RTE.ECO2MIX"
STAGE_PATH = "@RTE_STAGE/eCO2mix_RTE_En-cours-TR.csv"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

def download_zip():
    os.makedirs(os.path.dirname(LOCAL_ZIP_PATH), exist_ok=True)
    response = requests.get(URL)
    with open(LOCAL_ZIP_PATH, "wb") as f:
        f.write(response.content)

def unzip_csv():
    with zipfile.ZipFile(LOCAL_ZIP_PATH, 'r') as zip_ref:
        zip_ref.extractall(os.path.dirname(EXTRACTED_CSV_PATH))

def load_to_snowflake():
    # Lecture des colonnes
    with open(EXTRACTED_CSV_PATH, newline='', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=';')
        headers = next(reader)

    columns = ', '.join([f'"{col.strip()}"' for col in headers])

    conn = snowflake.connector.connect(
        user='HADJIRA25',
        password='42XCDpmzwMKxRww',
        account='TRMGRRV-JN45028',
        warehouse='COMPUTE_WH',
        database='BRONZE',
        schema='RTE'
    )

    cursor = conn.cursor()

    # Création de la table si elle n'existe pas
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE} (
        {', '.join([f'"{col.strip()}" STRING' for col in headers])}
    );
    """
    cursor.execute(create_table_sql)

    # Chargement depuis le stage Snowflake (supposé que le fichier y est déjà présent)
    copy_sql = f"""
    COPY INTO {SNOWFLAKE_TABLE} ({columns})
    FROM {STAGE_PATH}
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ';' SKIP_HEADER = 1)
    ON_ERROR = 'CONTINUE';
    """
    cursor.execute(copy_sql)

    cursor.close()
    conn.close()

with DAG(
    dag_id="eco2mix_to_snowflake",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["eco2mix", "snowflake"],
) as dag:

    t1 = PythonOperator(
        task_id="download_zip",
        python_callable=download_zip,
    )

    t2 = PythonOperator(
        task_id="unzip_csv",
        python_callable=unzip_csv,
    )

    t3 = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake,
    )

    t1 >> t2 >> t3
