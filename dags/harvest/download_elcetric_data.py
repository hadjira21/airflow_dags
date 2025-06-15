from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import subprocess
import pandas as pd
import os

DATA_DIR = "/opt/airflow/data"
CSV_FILE = os.path.join(DATA_DIR, "electric_data.csv")

def download_data():
    os.makedirs(DATA_DIR, exist_ok=True)
    url = "https://data.enedis.fr/api/explore/v2.1/catalog/datasets/production-electrique-par-filiere-a-la-maille-commune/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B"
    command = ["curl", "-L", "-o", CSV_FILE, url]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Fichier téléchargé avec succès : {CSV_FILE}")
    else:
        raise Exception(f"Erreur lors du téléchargement : {result.stderr}")

def read_data():
    if not os.path.exists(CSV_FILE):
        raise FileNotFoundError(f"Le fichier CSV n'existe pas : {CSV_FILE}")
    try:
        df = pd.read_csv(CSV_FILE, encoding='ISO-8859-1', delimiter=';')
        print("Aperçu des données :")
        print(df.head())
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier CSV : {e}")

def upload_to_snowflake():
    conn_params = {
        'user': 'HADJIRA25',
        'password': '42XCDpmzwMKxRww',
        'account': 'TRMGRRV-JN45028',
        'warehouse': 'COMPUTE_WH',
        'database': 'BRONZE',
        'schema': 'ENDIS'
    }
    df = pd.read_csv(CSV_FILE, encoding='ISO-8859-1', delimiter=';')

    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn", **conn_params)

    table_name = "electric_data"
    stage_name = "ENDIS_STAGE"  # Assure-toi que ce stage existe dans Snowflake

    conn = hook.get_conn()
    cursor = conn.cursor()

    # Générer la table si elle n'existe pas (basé sur df)
    dtype_mapping = {
        'object': 'VARCHAR',
        'float64': 'FLOAT',
        'int64': 'INT',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP'
    }
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
    copy_query = f"""
        COPY INTO {table_name}
        FROM @{stage_name}/{os.path.basename(CSV_FILE)}
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' FIELD_DELIMITER = ';' SKIP_HEADER = 1)
        ON_ERROR = 'CONTINUE';
    """
    hook.run(copy_query)
    print(f"Données insérées dans la table {table_name} via COPY INTO.")

default_args = {"owner": "airflow", "start_date": datetime(2025, 3, 20), "retries": 0,}

dag = DAG("download_and_process_electrique_data", default_args=default_args, schedule_interval="@daily", catchup=False,)

download_task = PythonOperator(task_id="download_csv_data", python_callable=download_data, dag=dag,)

read_task = PythonOperator(task_id="read_csv_data", python_callable=read_data, dag=dag,)

upload_task = PythonOperator(task_id="upload_to_snowflake", python_callable=upload_to_snowflake, dag=dag,)

download_task >> read_task >> upload_task

