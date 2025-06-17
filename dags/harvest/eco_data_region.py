from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
import os
import subprocess
import zipfile
import pandas as pd
import unidecode

BASE_DIR = "/opt/airflow/data"

# Dictionnaire région → URL
REGIONS = {
    "Auvergne-Rhone-Alpes": "https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_Auvergne-Rhone-Alpes_En-cours-TR.zip",
    "Bourgogne-Franche-Comte": "https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_Bourgogne-Franche-Comte_En-cours-TR.zip",
    "Bretagne": "https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_Bretagne_En-cours-TR.zip",
    "Grand-Est": "https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_Grand-Est_En-cours-TR.zip",
    "Ile-de-France": "https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_Ile-de-France_En-cours-TR.zip",
    "Nouvelle-Aquitaine": "https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_Nouvelle-Aquitaine_En-cours-TR.zip"
}

def download_data(region: str, url: str):
    region_dir = os.path.join(BASE_DIR, region)
    zip_file = os.path.join(region_dir, f"{region}.zip")
    os.makedirs(region_dir, exist_ok=True)
    command = ["curl", "-L", "-o", zip_file, url]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Fichier téléchargé avec succès pour {region} : {zip_file}")
    else:
        raise Exception(f"Erreur téléchargement {region}: {result.stderr}")

def unzip_data(region: str):
    region_dir = os.path.join(BASE_DIR, region)
    zip_file = os.path.join(region_dir, f"{region}.zip")
    if not os.path.exists(zip_file):
        raise FileNotFoundError(f"ZIP absent pour {region}: {zip_file}")
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(region_dir)
    print(f"Fichiers extraits pour {region} dans {region_dir}")

def rename_xls_to_csv(region: str):
    region_dir = os.path.join(BASE_DIR, region)
    xls_file = os.path.join(region_dir, f"eCO2mix_RTE_{region}_En-cours-TR.xls")
    csv_file = os.path.join(region_dir, f"{region}.csv")
    if os.path.exists(xls_file):
        os.rename(xls_file, csv_file)
        print(f"Fichier renommé pour {region} : {xls_file} → {csv_file}")
    else:
        raise FileNotFoundError(f"Fichier XLS introuvable pour {region}: {xls_file}")

def transform_data(region: str):
    region_dir = os.path.join(BASE_DIR, region)
    csv_file = os.path.join(region_dir, f"{region}.csv")
    if not os.path.exists(csv_file):
        raise FileNotFoundError(f"CSV introuvable pour {region} : {csv_file}")

    df = pd.read_csv(csv_file, encoding='ISO-8859-1', delimiter=';')
    df.columns = [unidecode.unidecode(col.strip()) for col in df.columns]

    SELECTED_COLUMNS = ["Perimetre", "Nature", "Date", "Heures", "Consommation", "Thermique", "Nucleaire", "Solaire", "Hydraulique"]
    selected_cols_clean = [unidecode.unidecode(col) for col in SELECTED_COLUMNS]
    df = df[[col for col in selected_cols_clean if col in df.columns]]

    # Supprime lignes avec valeurs invalides
    df = df[~df.isin(["-", "ND", "--", ""]).any(axis=1)]

    # Nettoyage accents
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].apply(lambda x: unidecode.unidecode(str(x)) if pd.notnull(x) else x)

    df.to_csv(csv_file, index=False, encoding='utf-8', sep='\t')
    print(f"Données transformées et exportées pour {region}")

def upload_to_snowflake(region: str):
    region_dir = os.path.join(BASE_DIR, region)
    csv_file = os.path.join(region_dir, f"{region}.csv")

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

    table_name = f"eco2_data_{region.replace('-', '_').replace(' ', '_').lower()}"

    create_table_sql = f"""
    CREATE OR REPLACE TABLE {table_name} (
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
    """
    snowflake_hook.run(create_table_sql)
    print(f"Table {table_name} créée ou remplacée.")

    stage_name = 'RTE_STAGE'
    put_command = f"PUT file://{csv_file} @{stage_name} OVERWRITE = TRUE"
    snowflake_hook.run(put_command)
    print(f"Fichier {csv_file} uploadé vers stage {stage_name}.")

    copy_query = f"""
    COPY INTO {table_name}
    FROM @{stage_name}/{region}.csv
    FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1, FIELD_DELIMITER = '\\t', TRIM_SPACE = TRUE, 
                   FIELD_OPTIONALLY_ENCLOSED_BY = '"', REPLACE_INVALID_CHARACTERS = TRUE, ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
    FORCE = TRUE
    ON_ERROR = 'CONTINUE';
    """
    snowflake_hook.run(copy_query)
    print(f"Données insérées dans la table {table_name} avec succès.")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 20),
    "retries": 0,
}

dag = DAG(
    "download_data_eco2mix_regions",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)

for region, url in REGIONS.items():
    with TaskGroup(group_id=f"{region}_tasks", dag=dag) as tg:
        download_task = PythonOperator(
            task_id=f"download_{region}",
            python_callable=download_data,
            op_kwargs={"region": region, "url": url},
        )
        unzip_task = PythonOperator(
            task_id=f"unzip_{region}",
            python_callable=unzip_data,
            op_kwargs={"region": region},
        )
        rename_task = PythonOperator(
            task_id=f"rename_{region}",
            python_callable=rename_xls_to_csv,
            op_kwargs={"region": region},
        )
        transform_task = PythonOperator(
            task_id=f"transform_{region}",
            python_callable=transform_data,
            op_kwargs={"region": region},
        )
        upload_task = PythonOperator(
            task_id=f"upload_{region}_to_snowflake",
            python_callable=upload_to_snowflake,
            op_kwargs={"region": region},
        )

        download_task >> unzip_task >> rename_task >> transform_task >> upload_task
