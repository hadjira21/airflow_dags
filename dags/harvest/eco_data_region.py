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

REGIONS = {
    "Auvergne-Rhone-Alpes": "https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_Auvergne-Rhone-Alpes_En-cours-TR.zip",
    "Bourgogne-Franche-Comte": "https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_Bourgogne-Franche-Comte_En-cours-TR.zip",
    "Bretagne": "https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_Bretagne_En-cours-TR.zip",
    "Grand-Est": "https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_Grand-Est_En-cours-TR.zip",
    "Ile-de-France": "https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_Ile-de-France_En-cours-TR.zip",
    "Nouvelle-Aquitaine": "https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_Nouvelle-Aquitaine_En-cours-TR.zip"
}

def get_paths(region):
    safe_region = region.replace(" ", "-")
    region_dir = os.path.join(BASE_DIR, safe_region)
    zip_file = os.path.join(region_dir, f"{safe_region}.zip")
    extracted_dir = region_dir
    xls_file = None
    # Le fichier XLS a un nom fixe pattern, on peut essayer de lister et prendre le premier xls
    if os.path.exists(extracted_dir):
        for f in os.listdir(extracted_dir):
            if f.endswith(".xls") and safe_region in f:
                xls_file = os.path.join(extracted_dir, f)
                break
    csv_file = os.path.join(extracted_dir, f"{safe_region}.csv")
    return region_dir, zip_file, extracted_dir, xls_file, csv_file

def download_data(region, url):
    region_dir, zip_file, _, _, _ = get_paths(region)
    os.makedirs(region_dir, exist_ok=True)
    command = ["curl", "-L", "-o", zip_file, url]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"[{region}] Fichier téléchargé avec succès : {zip_file}")
    else:
        raise Exception(f"[{region}] Erreur lors du téléchargement : {result.stderr}")

def unzip_data(region):
    region_dir, zip_file, extracted_dir, _, _ = get_paths(region)
    if not os.path.exists(zip_file):
        raise FileNotFoundError(f"[{region}] Le fichier ZIP n'existe pas : {zip_file}")
    os.makedirs(extracted_dir, exist_ok=True)
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(extracted_dir)
    print(f"[{region}] Fichiers extraits dans : {extracted_dir}")

def rename_xls_to_csv(region):
    region_dir, _, extracted_dir, xls_file, csv_file = get_paths(region)
    if not xls_file:
        # On essaye de trouver un XLS dans le dossier
        files = [f for f in os.listdir(extracted_dir) if f.endswith(".xls")]
        if files:
            xls_file = os.path.join(extracted_dir, files[0])
        else:
            raise FileNotFoundError(f"[{region}] Aucun fichier XLS trouvé dans {extracted_dir}")
    os.rename(xls_file, csv_file)
    print(f"[{region}] Fichier renommé de {xls_file} à {csv_file}")

def transform_data(region):
    _, _, _, _, csv_file = get_paths(region)
    if not os.path.exists(csv_file):
        raise FileNotFoundError(f"[{region}] Le fichier CSV est introuvable : {csv_file}")

    df = pd.read_csv(csv_file, encoding='ISO-8859-1', delimiter=';')
    df.columns = [unidecode.unidecode(col.strip()) for col in df.columns]

    SELECTED_COLUMNS = ["Perimetre", "Nature", "Date", "Heures", "Consommation", "Thermique", "Nucleaire", 'Solaire', 'Hydraulique']
    selected_cols_clean = [unidecode.unidecode(col) for col in SELECTED_COLUMNS]
    df = df[[col for col in selected_cols_clean if col in df.columns]]

    df = df[~df.isin(["-", "ND", "--", ""]).any(axis=1)]

    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].apply(lambda x: unidecode.unidecode(str(x)) if pd.notnull(x) else x)

    df.to_csv(csv_file, index=False, encoding='utf-8', sep='\t')
    print(f"[{region}] Données nettoyées, colonnes sélectionnées et exportées.")

def upload_to_snowflake(region):
    _, _, _, _, csv_file = get_paths(region)
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
        EOLIEN NUMBER,
        SOLAIRE NUMBER,
        HYDRAULIQUE NUMBER
    );
    """
    snowflake_hook.run(create_table_sql)

    stage_name = 'RTE_STAGE'
    put_command = f"PUT file://{csv_file} @{stage_name}"
    snowflake_hook.run(put_command)

    copy_query = f"""
    COPY INTO {table_name}
    FROM @{stage_name}/{os.path.basename(csv_file)}
    FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1, FIELD_DELIMITER = '\t', TRIM_SPACE = TRUE, 
    FIELD_OPTIONALLY_ENCLOSED_BY = '"', REPLACE_INVALID_CHARACTERS = TRUE, error_on_column_count_mismatch=false)
    FORCE = TRUE
    ON_ERROR = 'CONTINUE';
    """
    snowflake_hook.run(copy_query)
    print(f"[{region}] Données insérées avec succès dans Snowflake.")

# --- DAG ---

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 20),
    "retries": 0,
}

dag = DAG(
    "download_data_eco2mix_multiple_regions",
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
            dag=dag,
        )
        unzip_task = PythonOperator(
            task_id=f"unzip_{region}",
            python_callable=unzip_data,
            op_kwargs={"region": region},
            dag=dag,
        )
        rename_task = PythonOperator(
            task_id=f"rename_{region}",
            python_callable=rename_xls_to_csv,
            op_kwargs={"region": region},
            dag=dag,
        )
        transform_task = PythonOperator(
            task_id=f"transform_{region}",
            python_callable=transform_data,
            op_kwargs={"region": region},
            dag=dag,
        )
        upload_task = PythonOperator(
            task_id=f"upload_{region}_to_snowflake",
            python_callable=upload_to_snowflake,
            op_kwargs={"region": region},
            dag=dag,
        )

        download_task >> unzip_task >> rename_task >> transform_task >> upload_task
