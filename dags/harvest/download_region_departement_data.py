import os
import subprocess
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

DATA_DIR = "/opt/airflow/data"
CSV_FILE = os.path.join(DATA_DIR, "departements.csv")
CSV_URL = "https://www.data.gouv.fr/fr/datasets/r/70cef74f-70b1-495a-8500-c089229c0254"
TABLE_NAME = "departements_francais"
STAGE_NAME = "INSEE_STAGE"

def download_departements():
    os.makedirs(DATA_DIR, exist_ok=True)
    command = ["curl", "-L", "-o", CSV_FILE, CSV_URL]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Fichier téléchargé : {CSV_FILE}")
    else:
        raise Exception(f"Erreur téléchargement : {result.stderr}")

def preview_departements():
    if not os.path.exists(CSV_FILE):
        raise FileNotFoundError(f"Fichier introuvable : {CSV_FILE}")
    df = pd.read_csv(CSV_FILE, delimiter=',')
    print("Aperçu des départements :")
    print(df.head())

def upload_departements_to_snowflake():
    conn_params = {
        'user': 'HADJIRA25',
        'password': '42XCDpmzwMKxRww',
        'account': 'TRMGRRV-JN45028',
        'warehouse': 'INGESTION_WH',
        'database': 'INSEE',
        'schema': 'INSEE'
    }

    df = pd.read_csv(CSV_FILE, delimiter=',')
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn', **conn_params)

    conn = hook.get_conn()
    cursor = conn.cursor()

    dtype_mapping = {
        'object': 'VARCHAR',
        'float64': 'FLOAT',
        'int64': 'INT',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP'
    }

    # Création table si elle n'existe pas
    columns_sql = []
    for col in df.columns:
        col_type = dtype_mapping.get(str(df[col].dtype), 'VARCHAR')
        safe_col = col.replace(" ", "_").replace("-", "_").upper()
        columns_sql.append(f'"{safe_col}" {col_type}')
    create_sql = f'CREATE TABLE IF NOT EXISTS {TABLE_NAME} ({", ".join(columns_sql)});'

    try:
        cursor.execute(create_sql)
        print(f" Table `{TABLE_NAME}` créée ou déjà existante.")
    finally:
        cursor.close()

    # Upload dans le stage
    put_command = f"PUT file://{CSV_FILE} @{STAGE_NAME} OVERWRITE=TRUE"
    hook.run(put_command)

    # COPY INTO dans la table
    copy_query = f"""
        COPY INTO {TABLE_NAME}
        FROM @{STAGE_NAME}/{os.path.basename(CSV_FILE)}
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1)
        ON_ERROR = 'CONTINUE';
    """
    hook.run(copy_query)
    print(f"Données chargées dans la table `{TABLE_NAME}`")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 20),
    "retries": 0,
}

dag = DAG(
    "departements_to_snowflake",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="DAG pour charger les départements français dans Snowflake"
)

download_task = PythonOperator(
    task_id="download_departements",
    python_callable=download_departements,
    dag=dag
)

preview_task = PythonOperator(
    task_id="preview_departements",
    python_callable=preview_departements,
    dag=dag
)

upload_task = PythonOperator(
    task_id="upload_departements",
    python_callable=upload_departements_to_snowflake,
    dag=dag
)

download_task >> preview_task >> upload_task
