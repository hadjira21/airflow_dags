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
BASE_URL = "https://eco2mix.rte-france.com/download/eco2mix/"

# Liste des régions à traiter
REGIONS = [
    "Auvergne-Rhone-Alpes",
    "Bourgogne-Franche-Comte",
    "Occitanie",
]

def get_region_file_paths(region):
    """Retourne les chemins de fichiers pour une région donnée"""
    zip_filename = f"eCO2mix_RTE_{region}_En-cours-TR.zip"
    zip_file = os.path.join(DATA_DIR, zip_filename)
    extracted_dir = os.path.join(DATA_DIR, f"eCO2mix_RTE_{region}_En-cours-TR")
    xls_file = os.path.join(extracted_dir, f"eCO2mix_RTE_{region}_En-cours-TR.xls")
    csv_file = os.path.join(extracted_dir, f"eCO2mix_RTE_{region}_En-cours-TR.csv")
    
    return {
        'zip_file': zip_file,
        'extracted_dir': extracted_dir,
        'xls_file': xls_file,
        'csv_file': csv_file
    }

def download_data(region, **kwargs):
    """Télécharge le fichier ZIP depuis RTE pour une région spécifique."""
    file_paths = get_region_file_paths(region)
    
    os.makedirs(DATA_DIR, exist_ok=True)
    url = f"{BASE_URL}eCO2mix_RTE_{region}_En-cours-TR.zip"
    
    command = ["curl", "-L", "-o", file_paths['zip_file'], url]
    result = subprocess.run(command, capture_output=True, text=True)

    if result.returncode == 0:
        print(f"Fichier téléchargé avec succès pour {region}: {file_paths['zip_file']}")
    else:
        raise Exception(f"Erreur lors du téléchargement pour {region}: {result.stderr}")

# Modifiez de la même manière toutes les autres fonctions...

def unzip_data(**context):
    """Décompresse le fichier ZIP pour une région spécifique."""
    region = context['params']['region']
    file_paths = get_region_file_paths(region)

    if not os.path.exists(file_paths['zip_file']):
        raise FileNotFoundError(f"Le fichier ZIP n'existe pas : {file_paths['zip_file']}")

    os.makedirs(file_paths['extracted_dir'], exist_ok=True)
    with zipfile.ZipFile(file_paths['zip_file'], 'r') as zip_ref:
        zip_ref.extractall(file_paths['extracted_dir'])
    print(f"Fichiers extraits pour {region} dans : {file_paths['extracted_dir']}")

def rename_xls_to_csv(**context):
    """Renomme le fichier .xls en .csv pour une région spécifique."""
    region = context['params']['region']
    file_paths = get_region_file_paths(region)
    
    try:
        if os.path.exists(file_paths['xls_file']):
            os.rename(file_paths['xls_file'], file_paths['csv_file'])
            print(f"Fichier renommé de {file_paths['xls_file']} à {file_paths['csv_file']}")
        else:
            raise FileNotFoundError(f"Le fichier {file_paths['xls_file']} n'a pas été trouvé.")
    except Exception as e:
        print(f"Une erreur est survenue : {e}")

def read_data(**context):
    """Lit et affiche un aperçu des données pour une région spécifique."""
    region = context['params']['region']
    file_paths = get_region_file_paths(region)
    
    if not os.path.exists(file_paths['csv_file']):
        raise FileNotFoundError(f"Aucun fichier CSV trouvé : {file_paths['csv_file']}")

    try:
        df = pd.read_csv(file_paths['csv_file'], encoding='ISO-8859-1', delimiter=';')
        print(f"Aperçu des données pour {region}:")
        print(df.head())
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier CSV : {e}")

def transform_data(**context):
    """Supprime les accents des colonnes et des valeurs texte pour une région spécifique."""
    region = context['params']['region']
    file_paths = get_region_file_paths(region)
    
    if not os.path.exists(file_paths['csv_file']):
        raise FileNotFoundError(f"Le fichier CSV est introuvable : {file_paths['csv_file']}")

    try:
        df = pd.read_csv(file_paths['csv_file'], encoding='ISO-8859-1', delimiter=';')

        # Nettoyage des colonnes
        df.columns = [unidecode.unidecode(col.strip()) for col in df.columns]

        # Nettoyage des champs texte
        for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].apply(lambda x: unidecode.unidecode(str(x)) if pd.notnull(x) else x)

        df.to_csv(file_paths['csv_file'], index=False, encoding='utf-8', sep=';')
        print(f"Fichier transformé avec accents supprimés pour {region}.")
    except Exception as e:
        print(f"Erreur pendant la transformation : {e}")
        raise

def upload_to_snowflake(**context):
    """Charge les données dans Snowflake pour une région spécifique."""
    region = context['params']['region']
    file_paths = get_region_file_paths(region)
    
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

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS eco2_data_regional (
        REGION VARCHAR,
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
    print("Table vérifiée/créée avec succès dans Snowflake.")
    
    stage_name = 'RTE_STAGE'
    put_command = f"PUT file://{file_paths['csv_file']} @{stage_name}"
    snowflake_hook.run(put_command)
    
    # Ajout de la région comme première colonne
    copy_query = f"""
    COPY INTO eco2_data_regional
    FROM (
        SELECT 
            '{region}' as REGION,
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 
            $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, 
            $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, 
            $31, $32, $33, $34, $35, $36, $37, $38, $39, $40
        FROM @RTE_STAGE/{os.path.basename(file_paths['csv_file'])})
    FILE_FORMAT = (TYPE = 'CSV', 
        SKIP_HEADER = 1, 
        FIELD_DELIMITER = ';', 
        TRIM_SPACE = TRUE, 
        FIELD_OPTIONALLY_ENCLOSED_BY = '"', 
        REPLACE_INVALID_CHARACTERS = TRUE    )
    FORCE = TRUE
    ON_ERROR = 'CONTINUE';
    """
    
    snowflake_hook.run(copy_query)
    print(f"Données pour {region} insérées avec succès dans Snowflake.")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 20),
    "retries": 0,
}

dag = DAG(
    "download_data_eco2mix_regional",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2025, 6, 1),
    catchup=False,
)

for region in REGIONS:
    region_task_id = f"process_{region.lower().replace('-', '_')}"
    
    download_task = PythonOperator(
    task_id=f"download_{region_task_id}",
    python_callable=download_data,
    op_kwargs={'region': region},  
    dag=dag,
    )

    unzip_task = PythonOperator(
        task_id=f"unzip_{region_task_id}",
        python_callable=unzip_data,
        op_kwargs={'region': region}, 
        dag=dag,
    )

    rename_task = PythonOperator(
        task_id=f'rename_{region_task_id}',
        python_callable=rename_xls_to_csv,
        op_kwargs={'region': region},
        dag=dag,
    )

    read_task = PythonOperator(
        task_id=f"read_{region_task_id}",
        python_callable=read_data,
        op_kwargs={'region': region},
        dag=dag,
    )

    transform_task = PythonOperator(
        task_id=f"transform_{region_task_id}",
        python_callable=transform_data,
        op_kwargs={'region': region},
        dag=dag,
    )
    
    load_task = PythonOperator(
        task_id=f'upload_{region_task_id}',
        python_callable=upload_to_snowflake,
        op_kwargs={'region': region},
        dag=dag  
    )

    download_task >> unzip_task >> rename_task >> read_task >> transform_task #>> load_task