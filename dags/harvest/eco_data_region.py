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
CSV_PATH = "/opt/airflow/data/test_region/eCO2mix_RTE_En-cours-TR/eCO2mix_RTE_En-cours.csv"

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
        df.columns = [unidecode.unidecode(col.strip()).upper().replace(" ", "_").replace("-", "_") for col in df.columns]
        df.to_csv(CSV_PATH, index=False, encoding='utf-8', sep='\t')  # tab delimiter pour Snowflake
        print("Colonnes nettoyées et fichier transformé.")
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
    THERMIQUE NUMBER,
    NUCLEAIRE NUMBER,
    EOLIEN NUMBER,
    SOLAIRE NUMBER,
    HYDRAULIQUE NUMBER,
    POMPAGE NUMBER,
    BIOENERGIES NUMBER,
    STOCKAGE_BATTERIE NUMBER,
    DESTOCKAGE_BATTERIE NUMBER,
    EOLIEN_TERRESTRE NUMBER,
    EOLIEN_OFFSHORE NUMBER,
    ECH_PHYSIQUES NUMBER,
    FLUX_AURA_TO_AURA NUMBER,
    FLUX_BFC_TO_AURA NUMBER,
    FLUX_BRETAGNE_TO_AURA NUMBER,
    FLUX_CVL_TO_AURA NUMBER,
    FLUX_GE_TO_AURA NUMBER,
    FLUX_HDF_TO_AURA NUMBER,
    FLUX_IDF_TO_AURA NUMBER,
    FLUX_NORMANDIE_TO_AURA NUMBER,
    FLUX_NAQ_TO_AURA NUMBER,
    FLUX_OCCITANIE_TO_AURA NUMBER,
    FLUX_PDL_TO_AURA NUMBER,
    FLUX_PACA_TO_AURA NUMBER,
    FLUX_AURA_FROM_AURA NUMBER,
    FLUX_AURA_TO_BFC NUMBER,
    FLUX_AURA_TO_BRETAGNE NUMBER,
    FLUX_AURA_TO_CVL NUMBER,
    FLUX_AURA_TO_GE NUMBER,
    FLUX_AURA_TO_HDF NUMBER,
    FLUX_AURA_TO_IDF NUMBER,
    FLUX_AURA_TO_NORMANDIE NUMBER,
    FLUX_AURA_TO_NAQ NUMBER,
    FLUX_AURA_TO_OCCITANIE NUMBER,
    FLUX_AURA_TO_PDL NUMBER,
    FLUX_AURA_TO_PACA NUMBER,
    FLUX_ALLEMAGNE_TO_AURA NUMBER,
    FLUX_BELGIQUE_TO_AURA NUMBER,
    FLUX_ESPAGNE_TO_AURA NUMBER,
    FLUX_ITALIE_TO_AURA NUMBER,
    FLUX_LUXEMBOURG_TO_AURA NUMBER,
    FLUX_UK_TO_AURA NUMBER,
    FLUX_SUISSE_TO_AURA NUMBER,
    FLUX_AURA_TO_ALLEMAGNE NUMBER,
    FLUX_AURA_TO_BELGIQUE NUMBER,
    FLUX_AURA_TO_ESPAGNE NUMBER,
    FLUX_AURA_TO_ITALIE NUMBER,
    FLUX_AURA_TO_LUXEMBOURG NUMBER,
    FLUX_AURA_TO_UK NUMBER,
    FLUX_AURA_TO_SUISSE NUMBER,
    TCO_THERMIQUE NUMBER,
    TCH_THERMIQUE NUMBER,
    TCO_NUCLEAIRE NUMBER,
    TCH_NUCLEAIRE NUMBER,
    TCO_EOLIEN NUMBER,
    TCH_EOLIEN NUMBER,
    TCO_SOLAIRE NUMBER,
    TCH_SOLAIRE NUMBER,
    TCO_HYDRAULIQUE NUMBER,
    TCH_HYDRAULIQUE NUMBER,
    TCO_BIOENERGIES NUMBER,
    TCH_BIOENERGIES NUMBER
);

    """
    snowflake_hook.run(create_table_sql)
    print("Table créée dans Snowflake.")

    put_command = f"PUT file://{CSV_PATH} @RTE_STAGE"
    snowflake_hook.run(put_command)

    copy_query = """
COPY INTO eco2_data_test
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
        $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
        $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
        $31, $32, $33, $34, $35, $36, $37, $38, $39, $40,
        $41, $42, $43, $44, $45, $46, $47, $48, $49, $50,
        $51, $52, $53, $54, $55, $56, $57, $58, $59, $60,
        $61, $62, $63, $64, $65, $66, $67
    FROM @RTE_STAGE/eCO2mix_RTE_En-cours.csv
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
