from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import os
import subprocess
import zipfile
import pandas as pd
import unidecode

# Configuration des chemins
DATA_DIR = "/opt/airflow/data"
ZIP_FILE = os.path.join(DATA_DIR, "eCO2mix_RTE_En-cours-TR.zip")
EXTRACTED_DIR = os.path.join(DATA_DIR, "eCO2mix_RTE_En-cours-TR")
CSV_FILE = os.path.join(EXTRACTED_DIR, "eCO2mix_RTE_En-cours-TR.csv")

# Paramètres Snowflake
SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_SCHEMA = 'RTE'
SNOWFLAKE_TABLE = 'eco2mix_data'
SNOWFLAKE_STAGE = 'RTE_STAGE_ECO2MIX'

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
    print(f"Fichiers extraits dans : {EXTRACTED_DIR}")

def rename_xls_to_csv():
    """Renomme le fichier .xls en .csv."""
    xls_path = os.path.join(EXTRACTED_DIR, "eCO2mix_RTE_En-cours-TR.xls")
    if not os.path.exists(xls_path):
        raise FileNotFoundError(f"Le fichier {xls_path} n'a pas été trouvé.")
    
    os.rename(xls_path, CSV_FILE)
    print(f"Fichier renommé de {xls_path} à {CSV_FILE}")

def transform_data():
    """Nettoie les données et prépare pour Snowflake."""
    if not os.path.exists(CSV_FILE):
        raise FileNotFoundError(f"Le fichier CSV est introuvable : {CSV_FILE}")

    # Lecture des données
    df = pd.read_csv(CSV_FILE, encoding='ISO-8859-1', delimiter=';')
    
    # Nettoyage des colonnes
    df.columns = [unidecode.unidecode(col.strip()) for col in df.columns]
    
    # Conversion des virgules en points pour les nombres
    for col in df.columns:
        if df[col].dtype == 'object':
            try:
                df[col] = df[col].str.replace(',', '.').astype(float)
            except:
                pass
    
    # Sauvegarde du fichier nettoyé
    df.to_csv(CSV_FILE, index=False, encoding='utf-8', sep=';')
    print("Données transformées et prêtes pour Snowflake")

def upload_to_snowflake():
    conn_params = {
        'user': 'HADJIRABK',  
        'password' : '42XCDpmzwMKxRww',
        'account': 'OKVCAFF-IE00559',
        'warehouse': 'COMPUTE_WH', 
        'database': 'BRONZE', 
        'schema': "METEO"      
    }

    # Connexion à Snowflake
    hook = SnowflakeHook(
        snowflake_conn_id='snowflake_conn', 
        **conn_params 
    )
    hook.run(f"USE DATABASE {conn_params['database']}")
    hook.run(f"USE SCHEMA {conn_params['schema']}")
    
    # Création de la table si elle n'existe pas
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
        Perimetre STRING,
        Nature STRING,
        Date DATE,
        Heures STRING,
        Consommation FLOAT,
        Prevision_J_1 FLOAT,
        Prevision_J FLOAT,
        Fioul FLOAT,
        Charbon FLOAT,
        Gaz FLOAT,
        Nucleaire FLOAT,
        Eolien FLOAT,
        Solaire FLOAT,
        Hydraulique FLOAT,
        Pompage FLOAT,
        Bioenergies FLOAT,
        Echanges_physiques FLOAT,
        Taux_de_CO2 FLOAT,
        ECH_Comm_UK FLOAT,
        ECH_Comm_Espagne FLOAT,
        ECH_Comm_Italie FLOAT,
        ECH_Comm_Suisse FLOAT,
        ECH_Comm_Allemagne_Belgique FLOAT,
        Fioul_TAC FLOAT,
        Fioul_Cogeneration FLOAT,
        Fioul_Autres FLOAT,
        Gaz_TAC FLOAT,
        Gaz_Cogeneration FLOAT,
        Gaz_CCG FLOAT,
        Gaz_Autres FLOAT,
        Hydraulique_Fil_Eau FLOAT,
        Hydraulique_Lacs FLOAT,
        Hydraulique_STEP_Turbinage FLOAT,
        Bio_Dec FLOAT,
        Bio_Biomasse FLOAT,
        Bio_Biogaz FLOAT,
        Stockage_Batterie FLOAT,
        Destockage_Batterie FLOAT,
        Eolien_Terrestre FLOAT,
        Eolien_Offshore FLOAT
    )
    """
    hook.run(create_table_sql)
    
    # Création du stage si nécessaire
    hook.run(f"CREATE STAGE IF NOT EXISTS {SNOWFLAKE_STAGE}")
    
    # Upload du fichier vers le stage
    put_sql = f"PUT file://{CSV_FILE} @{SNOWFLAKE_STAGE}"
    hook.run(put_sql)
    
    # Copie des données dans la table
    copy_sql = f"""
    COPY INTO {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}
    FROM @{SNOWFLAKE_STAGE}/eCO2mix_RTE_En-cours-TR.csv
    FILE_FORMAT = (
        TYPE = CSV
        FIELD_DELIMITER = ';'
        SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        EMPTY_FIELD_AS_NULL = TRUE
        NULL_IF = ('NULL', 'null', '')
    )
    """
    hook.run(copy_sql)
    
    # Vérification
    count = hook.get_first(f"SELECT COUNT(*) FROM {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}")[0]
    print(f"Données chargées avec succès. Nombre d'enregistrements: {count}")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 20),
    "retries": 0,
}

dag = DAG(
    "eco2mix_etl_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="Pipeline complet de téléchargement, transformation et chargement des données eco2mix vers Snowflake"
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

rename_task = PythonOperator(
    task_id='rename_xls_to_csv',
    python_callable=rename_xls_to_csv,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    dag=dag,
)

upload_task = PythonOperator(
    task_id="upload_to_snowflake",
    python_callable=upload_to_snowflake,
    dag=dag,
)

download_task >> unzip_task >> rename_task >> transform_task >> upload_task
