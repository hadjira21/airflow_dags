from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from datetime import datetime
import os
import subprocess
import zipfile
import pandas as pd
import unidecode

# Définition des dossiers
DATA_DIR = "/opt/airflow/data"
ZIP_FILE = os.path.join(DATA_DIR, "eCO2mix_RTE_En-cours-TR.zip")
EXTRACTED_DIR = os.path.join(DATA_DIR, "eCO2mix_RTE_En-cours-TR")
CSV_FILE = os.path.join(EXTRACTED_DIR, "eCO2mix_RTE_En-cours-TR.csv")

# Configuration Snowflake
SNOWFLAKE_CONN_ID = "snowflake_conn"
SNOWFLAKE_SCHEMA = "RTE"
SNOWFLAKE_TABLE = "ECO2MIX"
SNOWFLAKE_STAGE = "RTE_STAGE"

def download_data():
    """Télécharge le fichier ZIP depuis RTE."""
    os.makedirs(DATA_DIR, exist_ok=True)
    url = "https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_En-cours-TR.zip"
    command = ["curl", "-L", "-o", ZIP_FILE, url]
    result = subprocess.run(command, capture_output=True, text=True)

    if result.returncode != 0:
        raise Exception(f"Erreur lors du téléchargement : {result.stderr}")
    print(f"Fichier téléchargé avec succès : {ZIP_FILE}")

def unzip_data():
    """Décompresse le fichier ZIP."""
    if not os.path.exists(ZIP_FILE):
        raise FileNotFoundError(f"Le fichier ZIP n'existe pas : {ZIP_FILE}")

    os.makedirs(EXTRACTED_DIR, exist_ok=True)
    with zipfile.ZipFile(ZIP_FILE, 'r') as zip_ref:
        zip_ref.extractall(EXTRACTED_DIR)
    
    # Renommer le fichier .xls en .csv
    xls_file = os.path.join(EXTRACTED_DIR, "eCO2mix_RTE_En-cours-TR.xls")
    if os.path.exists(xls_file):
        os.rename(xls_file, CSV_FILE)
    print(f"Fichiers extraits et renommés dans : {EXTRACTED_DIR}")

def transform_data():
    """Nettoie les données et supprime les accents."""
    if not os.path.exists(CSV_FILE):
        raise FileNotFoundError(f"Le fichier CSV est introuvable : {CSV_FILE}")

    try:
        df = pd.read_csv(CSV_FILE, encoding='ISO-8859-1', delimiter=';')

        # Nettoyage des colonnes
        df.columns = [unidecode.unidecode(col.strip().replace(' ', '_').replace('-', '_')) 
                     for col in df.columns]

        # Nettoyage des champs texte
        for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].apply(lambda x: unidecode.unidecode(str(x)) if pd.notnull(x) else x)

        df.to_csv(CSV_FILE, index=False, encoding='utf-8', sep=';')
        print("Fichier transformé avec succès.")
    except Exception as e:
        print(f"Erreur pendant la transformation : {e}")
        raise

def generate_create_table_sql():
    """Génère le SQL pour créer la table Snowflake."""
    if not os.path.exists(CSV_FILE):
        raise FileNotFoundError(f"CSV introuvable : {CSV_FILE}")

    df = pd.read_csv(CSV_FILE, delimiter=";", encoding="utf-8", nrows=100)  # Lire seulement un échantillon
    
    # Mapping des types pandas vers Snowflake
    type_mapping = {
        'int64': 'INTEGER',
        'float64': 'FLOAT',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP_NTZ',
        'object': 'VARCHAR'
    }
    
    columns = []
    for col, dtype in zip(df.columns, df.dtypes):
        col_name = col.upper().replace(' ', '_').replace('-', '_')
        snowflake_type = type_mapping.get(str(dtype), 'VARCHAR')
        columns.append(f'"{col_name}" {snowflake_type}')
    
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
        {',\n        '.join(columns)}
    );
    """
    return create_sql

def upload_to_stage():
    """Upload le fichier CSV vers le stage Snowflake."""
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    # Créer le stage s'il n'existe pas
    cursor.execute(f"CREATE STAGE IF NOT EXISTS {SNOWFLAKE_STAGE}")
    
    # Upload du fichier
    put_sql = f"PUT file://{CSV_FILE} @{SNOWFLAKE_STAGE} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
    cursor.execute(put_sql)
    print(f"Fichier {CSV_FILE} uploadé vers le stage {SNOWFLAKE_STAGE}")

def copy_into_table():
    """Copie les données du stage vers la table."""
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    copy_sql = f"""
    COPY INTO {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}
    FROM @{SNOWFLAKE_STAGE}/eCO2mix_RTE_En-cours-TR.csv
    FILE_FORMAT = (
        TYPE = 'CSV'
        FIELD_DELIMITER = ';'
        SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        NULL_IF = ('NULL', 'null', '')
    )
    ON_ERROR = 'CONTINUE'
    """
    cursor.execute(copy_sql)
    print(f"Données copiées du stage vers la table {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 20),
    "retries": 0,
}

dag = DAG(
    "eco2mix_data_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)

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

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    dag=dag,
)

create_table_task = SnowflakeOperator(
    task_id="create_table",
    sql=generate_create_table_sql(),
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    dag=dag,
)

upload_to_stage_task = PythonOperator(
    task_id="upload_to_stage",
    python_callable=upload_to_stage,
    dag=dag,
)

copy_into_table_task = PythonOperator(
    task_id="copy_into_table",
    python_callable=copy_into_table,
    dag=dag,
)

# Workflow
download_task >> unzip_task >> transform_task >> create_table_task
create_table_task >> upload_to_stage_task >> copy_into_table_task