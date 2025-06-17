from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
import os
import subprocess
import zipfile
import pandas as pd
import unidecode

# Chemins des fichiers
DATA_DIR = "/opt/airflow/data/test_region"
ZIP_FILE = os.path.join(DATA_DIR, "eCO2mix_RTE_En-cours-TR.zip")
EXTRACTED_DIR = os.path.join(DATA_DIR, "eCO2mix_RTE_En-cours-TR")
CSV_PATH = os.path.join(EXTRACTED_DIR, "eCO2mix_RTE_En-cours-TR.csv")

def download_data():
    os.makedirs(DATA_DIR, exist_ok=True)
    url = "https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_Auvergne-Rhone-Alpes_En-cours-TR.zip"
    command = ["curl", "-L", "-o", ZIP_FILE, url]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Erreur lors du téléchargement : {result.stderr}")
    print(f"Fichier téléchargé : {ZIP_FILE}")

def unzip_data():
    if not os.path.exists(ZIP_FILE):
        raise FileNotFoundError(f"Le fichier ZIP n'existe pas : {ZIP_FILE}")
    os.makedirs(EXTRACTED_DIR, exist_ok=True)
    with zipfile.ZipFile(ZIP_FILE, 'r') as zip_ref:
        zip_ref.extractall(EXTRACTED_DIR)
    print(f"Fichiers extraits dans : {EXTRACTED_DIR}")

def rename_xls_to_csv():
    try:
        for f in os.listdir(EXTRACTED_DIR):
            if f.endswith(".xls"):
                xls_path = os.path.join(EXTRACTED_DIR, f)
                os.rename(xls_path, CSV_PATH)
                print(f"Fichier renommé : {xls_path} -> {CSV_PATH}")
                return
        raise FileNotFoundError("Aucun fichier .xls trouvé pour le renommage.")
    except Exception as e:
        print(f"Erreur renommage : {e}")

def read_data():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"Aucun fichier CSV trouvé : {CSV_PATH}")
    try:
        df = pd.read_csv(CSV_PATH, encoding='ISO-8859-1', delimiter=';')
        print("Aperçu des données :")
        print(df.head())
    except Exception as e:
        print(f"Erreur lecture CSV : {e}")

def transform_data():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"Fichier CSV introuvable : {CSV_PATH}")
    try:
        df = pd.read_csv(CSV_PATH, encoding='ISO-8859-1', delimiter=';')
        df.columns = [unidecode.unidecode(col.strip()) for col in df.columns]
        for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].apply(lambda x: unidecode.unidecode(str(x)) if pd.notnull(x) else x)
        df.to_csv(CSV_PATH, index=False, encoding='utf-8', sep='\t')  # séparateur tab pour éviter les erreurs
        print("Fichier transformé avec accents supprimés.")
    except Exception as e:
        print(f"Erreur transformation : {e}")
        raise

def upload_to_snowflake():
    conn_params = {
        'user': 'HADJIRA25',
        'password': '42XCDpmzwMKxRww',
        'account': 'TRMGRRV-JN45028',
        'warehouse': 'INGESTION_WH',
        'database': 'BRONZE',
        'schema': 'RTE'
    }
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn', **conn_params)
    snowflake_hook.run(f"USE DATABASE {conn_params['database']}")
    snowflake_hook.run(f"USE SCHEMA {conn_params['schema']}")

    create_table_sql = """
    CREATE OR REPLACE TABLE eco2_data_test (
        PERIMETRE VARCHAR,
        NATURE VARCHAR,
        DATE DATE,
        HEURES TIME,
        CONSOMMATION NUMBER,
        PREVISION_J_1 NUMBER,
        PREVISION_J NUMBER,
        FIOUL NUMBER,
        CHARBON NUMBER,
        GAZ NUMBER,
        NUCLEAIRE NUMBER,
        EOLIEN NUMBER,
        SOLAIRE NUMBER,
        HYDRAULIQUE NUMBER,
        POMPAGE NUMBER,
        BIOENERGIES NUMBER,
        ECH_PHYSIQUES NUMBER,
        TAUX_DE_CO2 NUMBER,
        ECH_COMM_ANGLETERRE NUMBER,
        ECH_COMM_ESPAGNE NUMBER,
        ECH_COMM_ITALIE NUMBER,
        ECH_COMM_SUISSE NUMBER,
        ECH_COMM_ALLEMAGNE_BELGIQUE NUMBER,
        FIOUL_TAC NUMBER,
        FIOUL_COGEN NUMBER,
        FIOUL_AUTRES NUMBER,
        GAZ_TAC NUMBER,
        GAZ_COGEN NUMBER,
        GAZ_CCG NUMBER,
        GAZ_AUTRES NUMBER,
        HYDRO_FDE NUMBER,
        HYDRO_LACS NUMBER,
        HYDRO_STEP NUMBER,
        BIO_DECHETS NUMBER,
        BIO_BIOMASSE NUMBER,
        BIO_BIOGAZ NUMBER,
        STOCKAGE_BATTERIE NUMBER,
        DESTOCKAGE_BATTERIE NUMBER,
        EOLIEN_TERRESTRE NUMBER,
        EOLIEN_OFFSHORE NUMBER,
        CONSOMMATION_CORRIGEE NUMBER
    );
    """
    snowflake_hook.run(create_table_sql)
    print("Table créée dans Snowflake.")

    put_command = f"PUT file://{CSV_PATH} @RTE_STAGE"
    snowflake_hook.run(put_command)

    copy_query = """
    COPY INTO eco2_data_test
    FROM (
        SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
               $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
               $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
               $31, $32, $33, $34, $35, $36, $37, $38, $39, $40,
               $41
        FROM @RTE_STAGE/eCO2mix_RTE_En-cours-TR.csv
    )
    FILE_FORMAT = (
        TYPE = 'CSV',
        SKIP_HEADER = 1,
        FIELD_DELIMITER = '\t',
        TRIM_SPACE = TRUE,
        FIELD_OPTIONALLY_ENCLOSED_BY = '"',
        REPLACE_INVALID_CHARACTERS = TRUE
    )
    FORCE = TRUE
    ON_ERROR = 'CONTINUE';
    """
    snowflake_hook.run(copy_query)
    print("Données chargées dans Snowflake.")

# DAG définition
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 6, 1),
    "retries": 0,
}

dag = DAG(
    "download_data_eco2mix_test",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)

# Tasks
download_task = PythonOperator(task_id="download_data", python_callable=download_data, dag=dag)
unzip_task = PythonOperator(task_id="unzip_data", python_callable=unzip_data, dag=dag)
rename_task = PythonOperator(task_id="rename_xls_to_csv", python_callable=rename_xls_to_csv, dag=dag)
read_task = PythonOperator(task_id="read_data", python_callable=read_data, dag=dag)
transform_task = PythonOperator(task_id="transform_data", python_callable=transform_data, dag=dag)
load_task = PythonOperator(task_id="upload_to_snowflake", python_callable=upload_to_snowflake, dag=dag)

# Orchestration
download_task >> unzip_task >> rename_task >> read_task >> transform_task >> load_task
