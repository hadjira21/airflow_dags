import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

def upload_to_snowflake():
    conn_params = {
        'user': 'HADJIRABK',
        'password': '42XCDpmzwMKxRww',
        'account': 'OKVCAFF-IE00559',
        'warehouse': 'COMPUTE_WH',
        'database': 'BRONZE',
        'schema': 'METEO'
    }
    
    snowflake_hook = SnowflakeHook(
        snowflake_conn_id='snowflake_conn',
        **conn_params
    )

    # Utiliser la bonne base et schéma
    snowflake_hook.run(f"USE DATABASE {conn_params['database']}")
    snowflake_hook.run(f"USE SCHEMA {conn_params['schema']}")

    # Chemin du fichier CSV local
    file_path = '/opt/airflow/data/eCO2mix_RTE_En-cours-TR/eCO2mix_RTE_En-cours-TR.csv'
    stage_name = 'METEO_STAGE'

    # Upload fichier dans stage Snowflake (sans compression)
    put_command = f"PUT file://{file_path} @{stage_name}/eCO2mix_clean.csv.gz AUTO_COMPRESS=TRUE"
    snowflake_hook.run(put_command)

    # Copier les données depuis le stage vers la table Snowflake
    copy_query = """
    COPY INTO eco2mix_data
    FROM @METEO_STAGE/eCO2mix_clean.csv.gz
    FILE_FORMAT = (
        TYPE = 'CSV',
        FIELD_OPTIONALLY_ENCLOSED_BY = '"',
        FIELD_DELIMITER = '\t',
        DATE_FORMAT = 'YYYY-MM-DD',
        SKIP_HEADER = 1,
        ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    )
    ON_ERROR = 'CONTINUE';
    """
    COPY INTO eco2mix_data
    FROM @METEO_STAGE/eCO2mix_RTE_En-cours-TR.csv.gz
    FILE_FORMAT = METEO.TSV_FORMAT
    ON_ERROR = 'CONTINUE';
    snowflake_hook.run(copy_query)

# Définition du DAG Airflow
dag = DAG(
    'upload_eco2_to_snowflake',
    description='DAG pour uploader les données eco2 dans Snowflake',
    schedule_interval=None,  # Déclenché manuellement
    start_date=datetime(2025, 4, 24),
    catchup=False
)

upload_task = PythonOperator(
    task_id='upload_to_snowflake',
    python_callable=upload_to_snowflake,
    dag=dag
)

upload_task
