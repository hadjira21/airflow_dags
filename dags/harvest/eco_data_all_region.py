from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
import os
import subprocess
import zipfile
import pandas as pd
import unidecode

BASE_DIR = "/opt/airflow/data"
REGIONS = [
    "Auvergne-Rhone-Alpes",
    "Bretagne",
    "Nouvelle-Aquitaine",
]

def get_paths(region):
    region_dir = os.path.join(BASE_DIR, region)
    zip_file = os.path.join(region_dir, f"eCO2mix_RTE_{region}_En-cours-TR.zip")
    extracted_dir = region_dir
    xls_file = os.path.join(extracted_dir, f"eCO2mix_RTE_{region}_En-cours-TR.xls")
    csv_file = os.path.join(extracted_dir, f"{region}.csv")
    return {
        "region_dir": region_dir,
        "zip_file": zip_file,
        "extracted_dir": extracted_dir,
        "xls_file": xls_file,
        "csv_file": csv_file
    }

def download_data(region, **kwargs):
    paths = get_paths(region)
    os.makedirs(paths['region_dir'], exist_ok=True)
    url = f"https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_{region}_En-cours-TR.zip"
    command = ["curl", "-L", "-o", paths['zip_file'], url]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Fichier téléchargé avec succès : {paths['zip_file']}")
    else:
        raise Exception(f"Erreur lors du téléchargement : {result.stderr}")

def unzip_data(region, **kwargs):
    paths = get_paths(region)
    if not os.path.exists(paths['zip_file']):
        raise FileNotFoundError(f"Le fichier ZIP n'existe pas : {paths['zip_file']}")
    os.makedirs(paths['extracted_dir'], exist_ok=True)
    with zipfile.ZipFile(paths['zip_file'], 'r') as zip_ref:
        zip_ref.extractall(paths['extracted_dir'])
    print(f"Fichiers extraits dans : {paths['extracted_dir']}")

def rename_xls_to_csv(region, **kwargs):
    paths = get_paths(region)
    if os.path.exists(paths['xls_file']):
        os.rename(paths['xls_file'], paths['csv_file'])
        print(f"Fichier renommé de {paths['xls_file']} à {paths['csv_file']}")
    else:
        raise FileNotFoundError(f"Le fichier {paths['xls_file']} n'a pas été trouvé.")

def read_data(region, **kwargs):
    paths = get_paths(region)
    if not os.path.exists(paths['csv_file']):
        raise FileNotFoundError(f"Aucun fichier CSV trouvé : {paths['csv_file']}")
    df = pd.read_csv(paths['csv_file'], encoding='ISO-8859-1', delimiter=';')
    print(f"Aperçu des données pour {region} :")
    print(df.head())

def transform_data(region, **kwargs):
    paths = get_paths(region)
    if not os.path.exists(paths['csv_file']):
        raise FileNotFoundError(f"Le fichier CSV est introuvable : {paths['csv_file']}")

    df = pd.read_csv(paths['csv_file'], encoding='ISO-8859-1', delimiter=';')

    # Nettoyage des noms de colonnes
    df.columns = [unidecode.unidecode(col.strip()) for col in df.columns]

    SELECTED_COLUMNS = ["Perimetre", "Nature", "Date", "Heures", "Consommation", "Thermique", "Nucleaire", 'Solaire', 'Hydraulique']
    selected_cols_clean = [unidecode.unidecode(col) for col in SELECTED_COLUMNS]
    df = df[[col for col in selected_cols_clean if col in df.columns]]

    # Supprimer les lignes contenant '-', 'ND', '--', ou '' dans n'importe quelle colonne
    df = df[~df.isin(["-", "ND", "--", ""]).any(axis=1)]

    # Nettoyer les accents dans les colonnes texte
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].apply(lambda x: unidecode.unidecode(str(x)) if pd.notnull(x) else x)

    # Ajouter colonne région pour traçabilité
    df['Perimetre'] = region

    # Export CSV nettoyé
    df.to_csv(paths['csv_file'], index=False, encoding='utf-8', sep='\t')
    print(f"Données nettoyées et exportées pour {region}.")

def upload_to_snowflake(region, **kwargs):
    paths = get_paths(region)
    csv_file = paths['csv_file']

    if not os.path.exists(csv_file):
        raise FileNotFoundError(f"Fichier CSV introuvable : {csv_file}")

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

    snowflake_hook.run("""
        CREATE TABLE IF NOT EXISTS eco2_data_regional (
            PERIMETRE VARCHAR,
            NATURE VARCHAR,
            DATE DATE,
            HEURES TIME,
            CONSOMMATION NUMBER,
            THERMIQUE NUMBER,
            NUCLEAIRE NUMBER,
            SOLAIRE NUMBER,
            HYDRAULIQUE NUMBER
        );
    """)

    stage_name = 'RTE_STAGE'
    put_command = f"PUT file://{csv_file} @{stage_name} OVERWRITE = TRUE"
    snowflake_hook.run(put_command)

    copy_query = f"""
    COPY INTO eco2_data_regional
    FROM @{stage_name}/{os.path.basename(csv_file)}
    FILE_FORMAT = (
        TYPE = 'CSV',
        SKIP_HEADER = 1,
        FIELD_DELIMITER = '\\t',
        TRIM_SPACE = TRUE,
        FIELD_OPTIONALLY_ENCLOSED_BY = '"',
        REPLACE_INVALID_CHARACTERS = TRUE
    )
    FORCE = TRUE
    ON_ERROR = 'CONTINUE';
    """
    snowflake_hook.run(copy_query)
    print(f"Données pour {region} insérées avec succès dans Snowflake.")

# --- DAG setup ---
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 20),
    "retries": 0,
}

dag = DAG(
    "download_data_eco2mix_multiregions",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)

for region in REGIONS:
    region_key = region.lower().replace('-', '_').replace(' ', '_')

    download_task = PythonOperator(
        task_id=f"download_data_{region_key}",
        python_callable=download_data,
        op_kwargs={'region': region},
        dag=dag,
    )

    unzip_task = PythonOperator(
        task_id=f"unzip_data_{region_key}",
        python_callable=unzip_data,
        op_kwargs={'region': region},
        dag=dag,
    )

    rename_task = PythonOperator(
        task_id=f"rename_xls_to_csv_{region_key}",
        python_callable=rename_xls_to_csv,
        op_kwargs={'region': region},
        dag=dag,
    )

    read_task = PythonOperator(
        task_id=f"read_data_{region_key}",
        python_callable=read_data,
        op_kwargs={'region': region},
        dag=dag,
    )

    transform_task = PythonOperator(
        task_id=f"transform_data_{region_key}",
        python_callable=transform_data,
        op_kwargs={'region': region},
        dag=dag,
    )

    upload_task = PythonOperator(
        task_id=f"upload_to_snowflake_{region_key}",
        python_callable=upload_to_snowflake,
        op_kwargs={'region': region},
        dag=dag,
    )

    download_task >> unzip_task >> rename_task >> read_task >> transform_task >> upload_task
