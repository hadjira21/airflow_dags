from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
import os
import subprocess
import zipfile
import pandas as pd
import unidecode

# --- Constantes de chemin ---
BASE_DIR = "/opt/airflow/data"
REGION = "final_test"
REGION_DIR = os.path.join(BASE_DIR, REGION)
ZIP_FILE = os.path.join(REGION_DIR, f"{REGION}.zip")
EXTRACTED_DIR = REGION_DIR
XLS_FILE = os.path.join(EXTRACTED_DIR, "eCO2mix_RTE_Auvergne-Rhone-Alpes_En-cours-TR.xls")
CSV_FILE = os.path.join(EXTRACTED_DIR, f"{REGION}.csv")


def download_data():
    """Télécharge le fichier ZIP depuis RTE."""
    os.makedirs(REGION_DIR, exist_ok=True)
    url = "https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_Auvergne-Rhone-Alpes_En-cours-TR.zip"
    command = ["curl", "-L", "-o", ZIP_FILE, url]
    result = subprocess.run(command, capture_output=True, text=True)

    if result.returncode == 0:
        print(f"Fichier téléchargé avec succès : {ZIP_FILE}")
    else:
        raise Exception(f"Erreur lors du téléchargement : {result.stderr}")

def unzip_data():
    """Décompresse le fichier ZIP."""
    if not os.path.exists(ZIP_FILE):
        raise FileNotFoundError(f"Le fichier ZIP n'existe pas : {ZIP_FILE}")

    os.makedirs(EXTRACTED_DIR, exist_ok=True)
    with zipfile.ZipFile(ZIP_FILE, 'r') as zip_ref:
        zip_ref.extractall(EXTRACTED_DIR)
    print(f"Fichiers extraits dans : {EXTRACTED_DIR}")

def rename_xls_to_csv():
    """Renomme le fichier .xls en .csv."""
    if os.path.exists(XLS_FILE):
        os.rename(XLS_FILE, CSV_FILE)
        print(f"Fichier renommé de {XLS_FILE} à {CSV_FILE}")
    else:
        raise FileNotFoundError(f"Le fichier {XLS_FILE} n'a pas été trouvé.")

def read_data():
    """Lit et affiche un aperçu des données."""
    if not os.path.exists(CSV_FILE):
        raise FileNotFoundError(f"Aucun fichier CSV trouvé : {CSV_FILE}")

    df = pd.read_csv(CSV_FILE, encoding='ISO-8859-1', delimiter=';')
    print("Aperçu des données :")
    print(df.head())

def transform_data():
    """Supprime les accents des colonnes et des valeurs texte."""
    if not os.path.exists(CSV_FILE):
        raise FileNotFoundError(f"Le fichier CSV est introuvable : {CSV_FILE}")

    df = pd.read_csv(CSV_FILE, encoding='ISO-8859-1', delimiter=';')

    # Nettoyage des colonnes et valeurs
    df.columns = [unidecode.unidecode(col.strip()) for col in df.columns]
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].apply(lambda x: unidecode.unidecode(str(x)) if pd.notnull(x) else x)

    df.to_csv(CSV_FILE, index=False, encoding='utf-8', sep=';')
    print("Fichier transformé avec accents supprimés.")

def upload_to_snowflake():
    conn_params = {
        'user': 'HADJIRA25',
        'password': '42XCDpmzwMKxRww',
        'account': 'TRMGRRV-JN45028',
        'warehouse': 'INGESTION_WH',
        'database': 'BRONZE',
        'schema': "RTE"
    }
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn', **conn_params)
    snowflake_hook.run(f"USE DATABASE {conn_params['database']}")
    snowflake_hook.run(f"USE SCHEMA {conn_params['schema']}")

    snowflake_hook.run("""CREATE OR REPLACE TABLE eco2_data_test (
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
    STOCKAGE_BATTERIE VARCHAR,
    DESTOCKAGE_BATTERIE VARCHAR,
    EOLIEN_TERRESTRE VARCHAR,
    EOLIEN_OFFSHORE VARCHAR,
    ECH_PHYSIQUES NUMBER,
    FLUX_AURA_TO_AURA VARCHAR,
    FLUX_BFC_TO_AURA VARCHAR,
    FLUX_BRETAGNE_TO_AURA VARCHAR,
    FLUX_CVL_TO_AURA VARCHAR,
    FLUX_GE_TO_AURA VARCHAR,
    FLUX_HDF_TO_AURA VARCHAR,
    FLUX_IDF_TO_AURA VARCHAR,
    FLUX_NORMANDIE_TO_AURA VARCHAR,
    FLUX_NAQ_TO_AURA VARCHAR,
    FLUX_OCCITANIE_TO_AURA VARCHAR,
    FLUX_PDL_TO_AURA VARCHAR,
    FLUX_PACA_TO_AURA VARCHAR,
    FLUX_AURA_FROM_AURA VARCHAR,
    FLUX_AURA_TO_BFC VARCHAR,
    FLUX_AURA_TO_BRETAGNE VARCHAR,
    FLUX_AURA_TO_CVL VARCHAR,
    FLUX_AURA_TO_GE VARCHAR,
    FLUX_AURA_TO_HDF VARCHAR,
    FLUX_AURA_TO_IDF VARCHAR,
    FLUX_AURA_TO_NORMANDIE VARCHAR,
    FLUX_AURA_TO_NAQ VARCHAR,
    FLUX_AURA_TO_OCCITANIE VARCHAR,
    FLUX_AURA_TO_PDL VARCHAR,
    FLUX_AURA_TO_PACA VARCHAR,
    FLUX_ALLEMAGNE_TO_AURA VARCHAR,
    FLUX_BELGIQUE_TO_AURA VARCHAR,
    FLUX_ESPAGNE_TO_AURA VARCHAR,
    FLUX_ITALIE_TO_AURA VARCHAR,
    FLUX_LUXEMBOURG_TO_AURA VARCHAR,
    FLUX_UK_TO_AURA VARCHAR,
    FLUX_SUISSE_TO_AURA VARCHAR,
    FLUX_AURA_TO_ALLEMAGNE VARCHAR,
    FLUX_AURA_TO_BELGIQUE VARCHAR,
    FLUX_AURA_TO_ESPAGNE VARCHAR,
    FLUX_AURA_TO_ITALIE VARCHAR,
    FLUX_AURA_TO_LUXEMBOURG VARCHAR,
    FLUX_AURA_TO_UK VARCHAR,
    FLUX_AURA_TO_SUISSE VARCHAR,
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
""")

    stage_name = 'RTE_STAGE'
    put_command = f"PUT file://{CSV_FILE} @{stage_name}"
    snowflake_hook.run(put_command)

    copy_query = f"""
    COPY INTO eco2_data_test
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
           $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
           $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
           $31, $32, $33, $34, $35, $36, $37, $38, $39, $40,
           $41, $42, $43, $44, $45, $46, $47, $48, $49, $50,
           $51, $52, $53, $54, $55, $56, $57, $58, $59, $60,
           $61, $62, $63, $64, $65, $66, $67, $68
    FROM @RTE_STAGE/final_test.csv
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
    print("Données insérées avec succès dans Snowflake.")

# --- Définition du DAG ---
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 20),
    "retries": 0,
}

dag = DAG(
    "download_data_eco2mix_test",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)

# --- Définition des tâches ---
download_task = PythonOperator(task_id="download_data", python_callable=download_data, dag=dag)
unzip_task = PythonOperator(task_id="unzip_data", python_callable=unzip_data, dag=dag)
rename_task = PythonOperator(task_id='rename_xls_to_csv', python_callable=rename_xls_to_csv, dag=dag)
read_task = PythonOperator(task_id="read_data", python_callable=read_data, dag=dag)
transform_task = PythonOperator(task_id="transform_data", python_callable=transform_data, dag=dag)
upload_task = PythonOperator(task_id="upload_to_snowflake", python_callable=upload_to_snowflake, dag=dag)

# --- Orchestration ---
download_task >> unzip_task >> rename_task >> read_task >> transform_task >> upload_task
