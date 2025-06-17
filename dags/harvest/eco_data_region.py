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
DATA_DIR = "/opt/airflow/data/region"
BASE_URL = "https://eco2mix.rte-france.com/download/eco2mix/"

# Liste des régions à traiter
REGIONS = [
    "Auvergne-Rhone-Alpes",

]

def get_region_file_paths(region):
    """Retourne les chemins de fichiers pour une région donnée"""
    region_dir = os.path.join(DATA_DIR, "region") 
    zip_filename = f"eCO2mix_RTE_{region}_En-cours-TR.zip"
    zip_file = os.path.join(region_dir, zip_filename)
    extracted_dir = os.path.join(region_dir, f"eCO2mix_RTE_{region}_En-cours-TR")
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
    os.makedirs(os.path.join(DATA_DIR), exist_ok=True)
    os.makedirs(file_paths['extracted_dir'], exist_ok=True)
    
    url = f"{BASE_URL}eCO2mix_RTE_{region}_En-cours-TR.zip"
    
    command = ["curl", "-L", "-o", file_paths['zip_file'], url]
    result = subprocess.run(command, capture_output=True, text=True)

    if result.returncode == 0:
        print(f"Fichier téléchargé avec succès pour {region}: {file_paths['zip_file']}")
    else:
        raise Exception(f"Erreur lors du téléchargement pour {region}: {result.stderr}")

# Modifiez de la même manière toutes les autres fonctions...

def unzip_data(region, **kwargs):
    """Décompresse le fichier ZIP pour une région spécifique."""
    file_paths = get_region_file_paths(region)

    if not os.path.exists(file_paths['zip_file']):
        raise FileNotFoundError(f"Le fichier ZIP n'existe pas : {file_paths['zip_file']}")

    os.makedirs(file_paths['extracted_dir'], exist_ok=True)
    with zipfile.ZipFile(file_paths['zip_file'], 'r') as zip_ref:
        zip_ref.extractall(file_paths['extracted_dir'])
    print(f"Fichiers extraits pour {region} dans : {file_paths['extracted_dir']}")

def rename_xls_to_csv(region, **kwargs):
    """Renomme le fichier .xls en .csv pour une région spécifique."""

    file_paths = get_region_file_paths(region)
    
    try:
        if os.path.exists(file_paths['xls_file']):
            os.rename(file_paths['xls_file'], file_paths['csv_file'])
            print(f"Fichier renommé de {file_paths['xls_file']} à {file_paths['csv_file']}")
        else:
            raise FileNotFoundError(f"Le fichier {file_paths['xls_file']} n'a pas été trouvé.")
    except Exception as e:
        print(f"Une erreur est survenue : {e}")

def read_data(region, **kwargs):
    """Lit et affiche un aperçu des données pour une région spécifique."""
    file_paths = get_region_file_paths(region)
    
    if not os.path.exists(file_paths['csv_file']):
        raise FileNotFoundError(f"Aucun fichier CSV trouvé : {file_paths['csv_file']}")

    try:
        df = pd.read_csv(file_paths['csv_file'], encoding='ISO-8859-1', delimiter='\t')
        print(f"Aperçu des données pour {region}:")
        print(df.head())
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier CSV : {e}")

def upload_to_snowflake(region, **kwargs):
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

    file_paths = get_region_file_paths(region)
    
    if not os.path.exists(file_paths['csv_file']):
        raise FileNotFoundError(f"Fichier CSV introuvable : {file_paths['csv_file']}")

    df = pd.read_csv(file_paths['csv_file'], sep='\t', encoding='ISO-8859-1')  
    df = df[['Périmètre', 'Nature', 'Date', 'Heures', 'Consommation', 'Thermique', 'Eolien', 'Solaire', 'Hydraulique', 'Pompage']]

    dtype_mapping = {'object': 'VARCHAR',
        'float64': 'FLOAT',
        'int64': 'INT',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP'
    }

    columns_sql = []
    for col in df.columns:
        col_type = dtype_mapping.get(str(df[col].dtype), 'VARCHAR')
        safe_col = col.replace(" ", "_").replace("-", "_").replace("é","e").replace("É","E").upper()
        columns_sql.append(f'"{safe_col}" {col_type}')
    
    create_sql = f"""
    CREATE OR REPLACE TABLE eco2_data_regional (
        {', '.join(columns_sql)}
    );
    """

    snowflake_hook.run(create_sql)
    print("Table créée ou remplacée avec succès.")

    stage_name = 'RTE_STAGE'
    csv_file = file_paths['csv_file']
    csv_filename = os.path.basename(csv_file)

    put_command = f"PUT 'file://{csv_file}' @{stage_name} OVERWRITE = TRUE"
    snowflake_hook.run(put_command)
    print(f"Fichier chargé dans stage {stage_name}")

    nb_cols = len(df.columns)

# Récupérer les colonnes existantes dans la table Snowflake
    columns_query = """
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'RTE' AND TABLE_NAME = 'ECO2_DATA_REGIONAL'
    ORDER BY ORDINAL_POSITION
    """

    columns = snowflake_hook.get_pandas_df(columns_query)['COLUMN_NAME'].tolist()

    # Construire la requête COPY INTO avec ces colonnes
    copy_query = f"""
    COPY INTO eco2_data_regional ({', '.join(columns)})
    FROM @{stage_name}/{csv_filename}
    FILE_FORMAT = (
        TYPE = 'CSV',
        SKIP_HEADER = 1,
        FIELD_DELIMITER = ';',
        TRIM_SPACE = TRUE,
        FIELD_OPTIONALLY_ENCLOSED_BY = '"',
        REPLACE_INVALID_CHARACTERS = TRUE
    )
    FORCE = TRUE
    ON_ERROR = 'CONTINUE';
    """
    snowflake_hook.run(copy_query)



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

    
    load_task = PythonOperator(
        task_id=f'upload_{region_task_id}',
        python_callable=upload_to_snowflake,
        op_kwargs={'region': region},
        dag=dag  
    )

    download_task >> unzip_task >> rename_task >> read_task  >> load_task