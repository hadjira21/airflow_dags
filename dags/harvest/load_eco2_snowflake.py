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


def upload_to_snowflake(**kwargs):
    
    conn_params = {'user': 'HADJIRA25', 'password' : '42XCDpmzwMKxRww', 'account': 'TRMGRRV-JN45028',
    'warehouse': 'COMPUTE_WH', 'database': 'BRONZE',  'schema': "RTE" }
    file_path = '/opt/airflow/data/eCO2mix_RTE_En-cours-TR/eCO2mix_RTE_En-cours-TR.csv'
    table_name = 'ECO2MIX_DATA'
    stage_name = 'RTE_STAGE'
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn", **conn_params)
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
    database, schema = cursor.fetchone()
    cursor.close()

    full_table_name = f'{database}.{schema}.{table_name}'
    full_stage_name = f'{database}.{schema}.{stage_name}'

    # Création de la table
    create_table_sql = f"""
    CREATE OR REPLACE TABLE {full_table_name} (
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
    hook.run(create_table_sql)
    logging.info(f"📦 Table {full_table_name} créée (ou remplacée).")

    # PUT dans le stage
    put_command = f"PUT file://{file_path} @{full_stage_name} AUTO_COMPRESS=FALSE"
    hook.run(put_command)
    logging.info(f"📤 Fichier uploadé sur le stage {full_stage_name}")

    # COPY dans la table
    copy_query = f"""
    COPY INTO {full_table_name}
    FROM (
        SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, 
               $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, 
               $39, $40, $41
        FROM @{full_stage_name}
    )
    FILES = ('/opt/airflow/data/eCO2mix_RTE_En-cours-TR/eCO2mix_RTE_En-cours-TR.csv')
    FILE_FORMAT = (
        TYPE = 'CSV',
        SKIP_HEADER = 1,
        FIELD_DELIMITER = ',',
        TRIM_SPACE = TRUE,
        FIELD_OPTIONALLY_ENCLOSED_BY = '"',
        REPLACE_INVALID_CHARACTERS = TRUE
    )
    FORCE = TRUE
    ON_ERROR = 'ABORT_STATEMENT';
    """
    hook.run(copy_query)
    logging.info("✅ Données copiées dans la table Snowflake.")


default_args = {'owner': 'airflow', 'retries': 1,}

with DAG(
    dag_id='upload_eco2mix_to_snowflake_simple',
    default_args=default_args,
    description='Upload eCO2mix CSV to Snowflake without custom operator',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['eco2mix', 'snowflake'],
) as dag:

    load_task = PythonOperator(task_id='load_csv_to_snowflake',
        python_callable=upload_to_snowflake,
        provide_context=True,
    )

    load_task





