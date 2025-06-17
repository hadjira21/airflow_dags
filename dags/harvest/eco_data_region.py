from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
import os
import subprocess
import zipfile
import pandas as pd
import unidecode

# --- Configuration multi-régions ---
BASE_DIR = "/opt/airflow/data"
REGIONS = {
    "auvergne_rhone_alpes": "https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_Auvergne-Rhone-Alpes_En-cours-TR.zip",
    "ile-de-france": "https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_Ile-de-France_En-cours-TR.zip",
    'Bourgogne-Franche-Comte':"https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_Bourgogne-Franche-Comte_En-cours-TR.zip",
    'Bretagne':"https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_Bretagne_En-cours-TR.zip",
    'Centre-Val-de-Loire':"https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_Centre-Val-de-Loire_En-cours-TR.zip",
    'Grand-Est':"https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_Grand-Est_En-cours-TR.zip",
    'Hauts-de-France':"https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_Hauts-de-France_En-cours-TR.zip",
    'Normandie':"https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_Normandie_En-cours-TR.zip",
    'Nouvelle-Aquitaine':'https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_Nouvelle-Aquitaine_En-cours-TR.zip',
    'Occitanie':'https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_Occitanie_En-cours-TR.zip',
    'PACA':'https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_PACA_En-cours-TR.zip',
    'Pays-de-la-Loire':'https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_Pays-de-la-Loire_En-cours-TR.zip',
}

SELECTED_COLUMNS = ["Perimetre", "Nature", "Date", "Heures", "Consommation", "Thermique", "Nucleaire", 'Solaire', 'Hydraulique']

def download_and_extract_all():
    for region, url in REGIONS.items():
        region_dir = os.path.join(BASE_DIR, region)
        zip_path = os.path.join(region_dir, f"{region}.zip")

        os.makedirs(region_dir, exist_ok=True)

        # Télécharger le ZIP
        command = ["curl", "-L", "-o", zip_path, url]
        result = subprocess.run(command, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Erreur de téléchargement pour {region}: {result.stderr}")

        # Dézipper
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(region_dir)
        print(f"{region} téléchargé et extrait.")

def transform_and_combine_all():
    dfs = []
    for region in REGIONS.keys():
        region_dir = os.path.join(BASE_DIR, region)
        xls_file = [f for f in os.listdir(region_dir) if f.endswith(".xls")][0]
        xls_path = os.path.join(region_dir, xls_file)

        # Lire le fichier
        df = pd.read_csv(xls_path, encoding='ISO-8859-1', delimiter='\t')

        # Nettoyer les colonnes
        df.columns = [unidecode.unidecode(col.strip()) for col in df.columns]
        selected_cols_clean = [unidecode.unidecode(col) for col in SELECTED_COLUMNS]
        df = df[[col for col in selected_cols_clean if col in df.columns]]
        df = df[~df.isin(["-", "ND", "--", ""]).any(axis=1)]

        for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].apply(lambda x: unidecode.unidecode(str(x)) if pd.notnull(x) else x)

        df["region"] = region  # Ajouter une colonne pour la région
        dfs.append(df)

    # Concaténer toutes les régions
    combined_df = pd.concat(dfs, ignore_index=True)

    # Export en CSV global
    output_path = os.path.join(BASE_DIR, "eco2mix_combined.csv")
    combined_df.to_csv(output_path, index=False, encoding='utf-8', sep='\t')
    print("Données combinées exportées à :", output_path)

def upload_to_snowflake_combined():
    CSV_FILE = os.path.join(BASE_DIR, "eco2mix_combined.csv")
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
    CREATE OR REPLACE TABLE eco2_data_all (
        PERIMETRE VARCHAR,
        NATURE VARCHAR,
        DATE DATE,
        HEURES TIME,
        CONSOMMATION NUMBER,
        THERMIQUE NUMBER,
        NUCLEAIRE NUMBER,
        SOLAIRE NUMBER,
        HYDRAULIQUE NUMBER,
        REGION VARCHAR
    );
    """)

    stage_name = 'RTE_STAGE'
    snowflake_hook.run(f"REMOVE @{stage_name}/eco2mix_combined.csv")

    snowflake_hook.run(f"PUT file://{CSV_FILE} @{stage_name}")

    snowflake_hook.run(f"""
    COPY INTO eco2_data_all
    FROM @{stage_name}/eco2mix_combined.csv
    FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1, FIELD_DELIMITER = '\t', TRIM_SPACE = TRUE,
    FIELD_OPTIONALLY_ENCLOSED_BY = '"', REPLACE_INVALID_CHARACTERS = TRUE, error_on_column_count_mismatch=false)
    FORCE = TRUE
    ON_ERROR = 'CONTINUE';
    """)
    print("Données combinées insérées dans Snowflake.")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 20),
    "retries": 0,
}

dag = DAG(
    "eco2mix_multi_region_ingestion",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)

download_and_extract = PythonOperator(task_id="download_and_extract", python_callable=download_and_extract_all, dag=dag)
transform_and_combine = PythonOperator(task_id="transform_and_combine", python_callable=transform_and_combine_all, dag=dag)
upload_snowflake = PythonOperator(task_id="upload_to_snowflake", python_callable=upload_to_snowflake_combined, dag=dag)

download_and_extract >> transform_and_combine >> upload_snowflake
