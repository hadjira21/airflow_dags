import gzip
import shutil
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import subprocess
import pandas as pd
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas  # IMPORTANT: Needed for auto table creation

DATA_DIR = "/opt/airflow/data"
GZ_FILE = os.path.join(DATA_DIR, "meteo.csv.gz")
CSV_FILE = os.path.join(DATA_DIR, "meteo.csv")

def download_data():
    os.makedirs(DATA_DIR, exist_ok=True)
    url = "https://www.data.gouv.fr/fr/datasets/r/c1265c02-3a8e-4a28-961e-26b2fd704fe8"
    command = ["curl", "-L", "-o", GZ_FILE, url]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"✅ Fichier téléchargé : {GZ_FILE}")
    else:
        raise Exception(f"❌ Erreur téléchargement : {result.stderr}")

def decompress_file():
    if not os.path.exists(GZ_FILE):
        raise FileNotFoundError(f"❌ Fichier manquant : {GZ_FILE}")
    try:
        with gzip.open(GZ_FILE, 'rb') as f_in:
            with open(CSV_FILE, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print(f"✅ Fichier décompressé : {CSV_FILE}")
    except Exception as e:
        raise Exception(f"❌ Erreur décompression : {e}")

def read_data():
    if not os.path.exists(CSV_FILE):
        raise FileNotFoundError(f"❌ Fichier CSV non trouvé : {CSV_FILE}")
    try:
        df = pd.read_csv(CSV_FILE, encoding='ISO-8859-1', delimiter=';')
        print("✅ Aperçu des données :")
        print(df.head())
    except Exception as e:
        raise Exception(f"❌ Erreur lecture CSV : {e}")

def upload_to_snowflake():
    conn_params = {
        'user': 'HADJIRA25',
        'password': '42XCDpmzwMKxRww',
        'account': 'TRMGRRV-JN45028',
        'warehouse': 'COMPUTE_WH',
        'database': 'BRONZE',
        'schema': 'METEO'
    }

    # Lire le CSV en DataFrame
    df = pd.read_csv(CSV_FILE, encoding='ISO-8859-1', delimiter=';')

    # Connexion Snowflake via Hook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn', **conn_params)
    engine = hook.get_sqlalchemy_engine()

    # Création automatique de la table et insertion via write_pandas
    success, nchunks, nrows, _ = write_pandas(
        conn=engine.raw_connection(),
        df=df,
        table_name="meteo_data_test",
        schema=conn_params["schema"],
        database=conn_params["database"],
        auto_create_table=True,
        quote_identifiers=True
    )

    print(f"✅ Données insérées dans Snowflake : {nrows} lignes")

# DAG Definition
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 20),
    "retries": 0,
}

dag = DAG(
    "meteo_auto_to_snowflake",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="Téléchargement, décompression et chargement automatique dans Snowflake",
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
    task_id="read_csv_preview",
    python_callable=read_data,
    dag=dag,
)

upload_task = PythonOperator(
    task_id="upload_to_snowflake",
    python_callable=upload_to_snowflake,
    dag=dag,
)

# Task dependencies
download_task >> decompress_task >> read_task >> upload_task
