

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

# Fonction d'insertion dans Snowflake
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
    snowflake_hook = SnowflakeHook(
        snowflake_conn_id='snowflake_conn', 
        **conn_params 
    )
    snowflake_hook.run(f"USE DATABASE {conn_params['database']}")
    snowflake_hook.run(f"USE SCHEMA {conn_params['schema']}")
  
    # Créer la table si elle n'existe pas
    create_table_sql = """ CREATE TABLE IF NOT EXISTS meteo_data (
    NUM_POSTE VARCHAR,
    NOM_USUEL VARCHAR,
    LAT FLOAT,
    LON FLOAT,
    ALTI FLOAT,
    AAAAMMJJ DATE,
    RR FLOAT,
    QRR INTEGER,
    TN FLOAT,
    QTN INTEGER,
    HTN TIME,
    QHTN INTEGER,
    TX FLOAT,
    QTX INTEGER,
    HTX TIME,
    QHTX INTEGER,
    TM FLOAT,
    QTM INTEGER,
    TNTXM FLOAT,
    QTNTXM INTEGER,
    TAMPLI FLOAT,
    QTAMPLI INTEGER,
    TNSOL FLOAT,
    QTNSOL INTEGER,
    TN50 FLOAT,
    QTN50 INTEGER,
    DG FLOAT,
    QDG INTEGER,
    FFM FLOAT,
    QFFM INTEGER,
    FF2M FLOAT,
    QFF2M INTEGER,
    FXY FLOAT,
    QFXY INTEGER,
    DXY FLOAT,
    QDXY INTEGER,
    HXY TIME,
    QHXY INTEGER,
    FXI FLOAT,
    QFXI INTEGER,
    DXI FLOAT,
    QDXI INTEGER,
    HXI TIME,
    QHXI INTEGER,
    FXI2 FLOAT,
    QFXI2 INTEGER,
    DXI2 FLOAT,
    QDXI2 INTEGER,
    HXI2 TIME,
    QHXI2 INTEGER,
    FXI3S FLOAT,
    QFXI3S INTEGER,
    DXI3S FLOAT,
    QDXI3S INTEGER,
    HXI3S TIME,
    QHXI3S INTEGER,
    DRR FLOAT,
    QDRR INTEGER
);
    """
    snowflake_hook.run(create_table_sql)

    # Charger le fichier CSV dans le stage interne
    file_path = '/opt/airflow/data/meteo_data.csv'
    stage_name = 'METEO_STAGE'

    # Utiliser la commande PUT pour charger le fichier dans le stage
    put_command = f"PUT file://{file_path} @{stage_name}"
    snowflake_hook.run(put_command)
    
    # Copier les données depuis le stage dans la table Snowflake
    copy_query = """
    COPY INTO meteo_data
    FROM @METEO_STAGE/meteo_data.csv
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"', FIELD_DELIMITER = ';')
    ON_ERROR = 'CONTINUE';
"""
    snowflake_hook.run(copy_query)

    print("✅ Données insérées avec succès dans Snowflake.")

# Définir le DAG Airflow
dag = DAG(
    'upload_meteo_data_to_snowflake',
    description='DAG pour uploader les données  METEO dans Snowflake',
    schedule_interval='@daily',
    start_date=datetime(2025, 4, 24),
    catchup=False
)
upload_task = PythonOperator(
    task_id='upload_to_snowflake',
    python_callable=upload_to_snowflake,
    dag=dag
)
upload_task
