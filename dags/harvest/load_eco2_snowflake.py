import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago
import logging
# def upload_to_snowflake():
#     conn_params = {'user': 'HADJIRA25', 'password' : '42XCDpmzwMKxRww', 'account': 'TRMGRRV-JN45028',
#     'warehouse': 'COMPUTE_WH', 'database': 'BRONZE',  'schema': "RTE" }
#     snowflake_hook = SnowflakeHook(
#         snowflake_conn_id='snowflake_conn',
#         **conn_params
#     )

#     # Utiliser la bonne base et schéma
#     snowflake_hook.run(f"USE DATABASE {conn_params['database']}")
#     snowflake_hook.run(f"USE SCHEMA {conn_params['schema']}")

#     # Chemin du fichier CSV local
#     file_path = '/opt/airflow/data/eCO2mix_RTE_En-cours-TR/eCO2mix_RTE_En-cours-TR.csv'
#     stage_name = 'RTE_STAGE'

#     # Upload fichier dans stage Snowflake (sans compression)
#     put_command = f"PUT file://{file_path} @{stage_name}/eCO2mix_clean.csv.gz AUTO_COMPRESS=TRUE"
#     snowflake_hook.run(put_command)

#     # Copier les données depuis le stage vers la table Snowflake
#     copy_query = """
# COPY INTO eco2mix_data
# FROM @METEO_STAGE/eCO2mix_RTE_En-cours-TR.csv.gz
# FILE_FORMAT = (
#     TYPE = 'CSV',
#     FIELD_DELIMITER = '\t',
#     FIELD_OPTIONALLY_ENCLOSED_BY = '"',
#     DATE_FORMAT = 'YYYY-MM-DD',
#     SKIP_HEADER = 1,
#     ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
# )
# ON_ERROR = 'CONTINUE';

#     """

#     snowflake_hook.run(copy_query)

def upload_to_snowflake():
    conn_params = {'user': 'HADJIRA25', 'password' : '42XCDpmzwMKxRww', 'account': 'TRMGRRV-JN45028',
    'warehouse': 'COMPUTE_WH', 'database': 'BRONZE',  'schema': "RTE" }
    snowflake_hook = SnowflakeHook( snowflake_conn_id='snowflake_conn', **conn_params)
    snowflake_hook.run(f"USE DATABASE {conn_params['database']}")
    snowflake_hook.run(f"USE SCHEMA {conn_params['schema']}")

    create_table_sql = f"""
    CREATE OR REPLACE TABLE eco2_data (
        "Périmètre" VARCHAR,
        "Nature" VARCHAR,
        "Date" VARCHAR,
        "Heures" VARCHAR,
        "Consommation" VARCHAR,
        "Prévision J-1" VARCHAR,
        "Prévision J" VARCHAR,
        "Fioul" VARCHAR,
        "Charbon" VARCHAR,
        "Gaz" VARCHAR,
        "Nucléaire" VARCHAR,
        "Eolien" VARCHAR,
        "Solaire" VARCHAR,
        "Hydraulique" VARCHAR,
        "Pompage" VARCHAR,
        "Bioénergies" VARCHAR,
        "Ech. physiques" VARCHAR,
        "Taux de Co2" VARCHAR,
        "Ech. comm. Angleterre" VARCHAR,
        "Ech. comm. Espagne" VARCHAR,
        "Ech. comm. Italie" VARCHAR,
        "Ech. comm. Suisse" VARCHAR,
        "Ech. comm. Allemagne-Belgique" VARCHAR,
        "Fioul - TAC" VARCHAR,
        "Fioul - Cogén." VARCHAR,
        "Fioul - Autres" VARCHAR,
        "Gaz - TAC" VARCHAR,
        "Gaz - Cogén." VARCHAR,
        "Gaz - CCG" VARCHAR,
        "Gaz - Autres" VARCHAR,
        "Hydraulique - Fil de l?eau + éclusée" VARCHAR,
        "Hydraulique - Lacs" VARCHAR,
        "Hydraulique - STEP turbinage" VARCHAR,
        "Bioénergies - Déchets" VARCHAR,
        "Bioénergies - Biomasse" VARCHAR,
        "Bioénergies - Biogaz" VARCHAR,
        "Stockage batterie" VARCHAR,
        "Déstockage batterie" VARCHAR,
        "Eolien terrestre" VARCHAR,
        "Eolien offshore" VARCHAR,
        "Consommation corrigée" VARCHAR
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
    FILE_FORMAT = (
        TYPE = 'CSV',
        SKIP_HEADER = 1,
        FIELD_DELIMITER = '/t',
        TRIM_SPACE = TRUE,
        FIELD_OPTIONALLY_ENCLOSED_BY = '"',
        REPLACE_INVALID_CHARACTERS = TRUE
    )
    FORCE = TRUE
    ON_ERROR = 'ABORT_STATEMENT';
    """
    snowflake_hook.run(copy_query)
    print("Données insérées avec succès dans Snowflake.")


default_args = {'owner': 'airflow', 'retries': 1,}
dag = DAG(
    "upload_eco2mix_to_snowflake_simple",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2025, 6, 1),
    catchup=False,
)
load_task = PythonOperator(
    task_id='upload_to_snowflake',
    python_callable=upload_to_snowflake,
    provide_context=True,
    dag=dag  
)

load_task





