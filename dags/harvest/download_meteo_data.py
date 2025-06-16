import gzip
import shutil
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import subprocess
import pandas as pd
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas 

DATA_DIR = "/opt/airflow/data"
GZ_FILE = os.path.join(DATA_DIR, "meteo.csv.gz")
CSV_FILE = os.path.join(DATA_DIR, "meteo.csv")

def download_data():
    os.makedirs(DATA_DIR, exist_ok=True)
    url = "https://www.data.gouv.fr/fr/datasets/r/c1265c02-3a8e-4a28-961e-26b2fd704fe8"
    command = ["curl", "-L", "-o", GZ_FILE, url]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Fichier téléchargé : {GZ_FILE}")
    else:
        raise Exception(f" Erreur téléchargement : {result.stderr}")

def decompress_file():
    if not os.path.exists(GZ_FILE):
        raise FileNotFoundError(f" Fichier manquant : {GZ_FILE}")
    try:
        with gzip.open(GZ_FILE, 'rb') as f_in:
            with open(CSV_FILE, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print(f" Fichier décompressé : {CSV_FILE}")
    except Exception as e:
        raise Exception(f" Erreur décompression : {e}")

def read_data():
    if not os.path.exists(CSV_FILE):
        raise FileNotFoundError(f" Fichier CSV non trouvé : {CSV_FILE}")
    try:
        df = pd.read_csv(CSV_FILE, encoding='ISO-8859-1', delimiter=';')
        print(" Aperçu des données :")
        print(df.head())
    except Exception as e:
        raise Exception(f" Erreur lecture CSV : {e}")

def upload_to_snowflake():
    conn_params = {'user': 'HADJIRA25', 'password' : '42XCDpmzwMKxRww', 'account': 'TRMGRRV-JN45028',
    'warehouse': 'INGESTION_WH', 'database': 'BRONZE',  'schema': "METEO" }
    df = pd.read_csv(CSV_FILE, encoding='ISO-8859-1', delimiter=';')
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn', **conn_params)
    
    table_name = "meteo_data"
    stage_name = "METEO_STAGE"  

    conn = hook.get_conn()
    cursor = conn.cursor()

    dtype_mapping = {'object': 'VARCHAR', 'float64': 'FLOAT', 'int64': 'INT', 'bool': 'BOOLEAN', 'datetime64[ns]': 'TIMESTAMP' }
    columns_sql = []
    for col in df.columns:
        col_type = dtype_mapping.get(str(df[col].dtype), 'VARCHAR')
        safe_col = col.replace(" ", "_").replace("-", "_").upper()
        columns_sql.append(f'"{safe_col}" {col_type}')
    create_sql = f'CREATE TABLE IF NOT EXISTS {table_name} ({", ".join(columns_sql)});'

    try:
        cursor.execute(create_sql)
        print(f"Table `{table_name}` créée ou existante.")
    finally:
        cursor.close()

    # PUT du fichier CSV dans le stage
    put_command = f"PUT file://{CSV_FILE} @{stage_name} OVERWRITE=TRUE"
    hook.run(put_command)
    print(f"Fichier {CSV_FILE} chargé dans le stage {stage_name}.")

    # COPY INTO pour insérer les données dans la table
    copy_query = f""" COPY INTO {table_name} FROM @{stage_name}/{os.path.basename(CSV_FILE)}
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' FIELD_DELIMITER = ';' SKIP_HEADER = 1) ON_ERROR = 'CONTINUE'; """
    hook.run(copy_query)
    print(f"Données insérées dans la table {table_name} via COPY INTO.")

default_args = {"owner": "airflow", "start_date": datetime(2024, 3, 20), "retries": 0, }
dag = DAG("meteo_auto_to_snowflake", default_args=default_args, schedule_interval="@daily", catchup=False,
    description="Téléchargement, décompression et chargement automatique dans Snowflake", )
download_task = PythonOperator(task_id="download_gz_csv", python_callable=download_data, dag=dag,)
decompress_task = PythonOperator(task_id="decompress_gz_csv", python_callable=decompress_file, dag=dag,)
read_task = PythonOperator(task_id="read_csv_preview", python_callable=read_data, dag=dag,)
upload_task = PythonOperator(task_id="upload_to_snowflake",python_callable=upload_to_snowflake,dag=dag,)
download_task >> decompress_task >> read_task >> upload_task
