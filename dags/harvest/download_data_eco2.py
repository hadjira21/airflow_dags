from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
import os
import subprocess
import zipfile
import pandas as pd
import unidecode 

# Définition du dossier de stockage
DATA_DIR = "/opt/airflow/data"
ZIP_FILE = os.path.join(DATA_DIR, "eCO2mix_RTE_En-cours-TR.zip")
EXTRACTED_DIR = os.path.join(DATA_DIR, "eCO2mix_RTE_En-cours-TR")
CSV_DIR = os.path.join(DATA_DIR, "csv_files")  # Dossier pour les fichiers CSV

def download_data():
    """Télécharge le fichier ZIP depuis RTE."""
    os.makedirs(DATA_DIR, exist_ok=True)
    url = "https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_En-cours-TR.zip"
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

def rename_xls_to_csv(xls_path, csv_path):
    """Renomme le fichier .xls en .csv."""
    try:
        if os.path.exists(xls_path):
            os.rename(xls_path, csv_path)
            print(f"Fichier renommé de {xls_path} à {csv_path}")
        else:
            raise FileNotFoundError(f"Le fichier {xls_path} n'a pas été trouvé.")
    except Exception as e:
        print(f"Une erreur est survenue : {e}")

def read_data():
    """Lit et affiche un aperçu des données."""
    csv_path = os.path.join(EXTRACTED_DIR, "eCO2mix_RTE_En-cours-TR.csv")
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Aucun fichier CSV trouvé : {csv_path}")

    try:
        df = pd.read_csv(csv_path, encoding='ISO-8859-1', delimiter=';')
        print("Aperçu des données :")
        print(df.head())
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier CSV : {e}")

def transform_data():
    """Supprime les accents des colonnes et des valeurs texte."""
    csv_path = os.path.join(EXTRACTED_DIR, "eCO2mix_RTE_En-cours-TR.csv")
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Le fichier CSV est introuvable : {csv_path}")

    try:
        df = pd.read_csv(csv_path, encoding='ISO-8859-1', delimiter=';')

        # Nettoyage des colonnes
        df.columns = [unidecode.unidecode(col.strip()) for col in df.columns]

        # Nettoyage des champs texte
        for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].apply(lambda x: unidecode.unidecode(str(x)) if pd.notnull(x) else x)

        df.to_csv(csv_path, index=False, encoding='utf-8', sep=';')
        print("Fichier transformé avec accents supprimés.")
    except Exception as e:
        print(f"Erreur pendant la transformation : {e}")
        raise

def upload_to_snowflake():
    conn_params = {'user': 'HADJIRA25', 'password' : '42XCDpmzwMKxRww', 'account': 'TRMGRRV-JN45028',
    'warehouse': 'COMPUTE_WH', 'database': 'BRONZE',  'schema': "RTE" }
    snowflake_hook = SnowflakeHook( snowflake_conn_id='snowflake_conn', **conn_params)
    snowflake_hook.run(f"USE DATABASE {conn_params['database']}")
    snowflake_hook.run(f"USE SCHEMA {conn_params['schema']}")

    create_table_sql = f"""
 CREATE OR REPLACE TABLE eco2_data (
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
    print("Table crée avec succès dans Snowflake.")
    file_path = '/opt/airflow/data/eCO2mix_RTE_En-cours-TR/eCO2mix_RTE_En-cours-TR.csv'
    stage_name = 'RTE_STAGE'
    put_command = f"PUT file://{file_path} @{stage_name}"
    snowflake_hook.run(put_command)
    copy_query = f"""
    COPY INTO eco2_data
    FROM (SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, 
    $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, 
    $39, $40, $41 FROM @RTE_STAGE/eCO2mix_RTE_En-cours-TR.csv )
    FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1, FIELD_DELIMITER = '\t', TRIM_SPACE = TRUE, FIELD_OPTIONALLY_ENCLOSED_BY = '"', REPLACE_INVALID_CHARACTERS = TRUE)
    FORCE = TRUE
    ON_ERROR = 'CONTINUE';"""
    snowflake_hook.run(copy_query)
    print("Données insérées avec succès dans Snowflake.")
default_args = { "owner": "airflow", "start_date": datetime(2025, 3, 20), "retries": 0,}

dag = DAG("download_data_eco2mix", default_args=default_args, 
    schedule_interval="@daily",
    start_date=datetime(2025, 6, 1),
    catchup=False,
)
xls_file_path = os.path.join(EXTRACTED_DIR, "eCO2mix_RTE_En-cours-TR.xls")
csv_file_path = os.path.join(EXTRACTED_DIR, "eCO2mix_RTE_En-cours-TR.csv")

download_task = PythonOperator(task_id="download_data_eco2", python_callable=download_data,  dag=dag,)

unzip_task = PythonOperator( task_id="unzip_data", python_callable=unzip_data, dag=dag,)

rename_task = PythonOperator(task_id='rename_xls_to_csv', python_callable=rename_xls_to_csv,op_args=[xls_file_path, csv_file_path],dag=dag,)

read_task = PythonOperator(task_id="read_data", python_callable=read_data, dag=dag,)

transform_task = PythonOperator(task_id="transform_data",    python_callable=transform_data,  dag=dag,)
load_task = PythonOperator(
    task_id='upload_to_snowflake',
    python_callable=upload_to_snowflake,
    provide_context=True,
    dag=dag  
)
download_task >> unzip_task >> rename_task >> read_task >> transform_task >> load_task