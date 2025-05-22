import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import os

def upload_to_snowflake():
    # Configuration de la connexion Snowflake
    conn_params = {
        'user': 'HADJIRABK',
        'password': '42XCDpmzwMKxRww',
        'account': 'OKVCAFF-IE00559',
        'warehouse': 'COMPUTE_WH',
        'database': 'BRONZE',
        'schema': 'RTE'
    }

    # Connexion à Snowflake
    snowflake_hook = SnowflakeHook(
        snowflake_conn_id='snowflake_conn',
        **conn_params
    )

    # Sélection de la base et du schéma
    snowflake_hook.run(f"USE DATABASE {conn_params['database']}")
    snowflake_hook.run(f"USE SCHEMA {conn_params['schema']}")

    # Création de la table (structure doit correspondre au fichier CSV)
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS eco2mix_data (
        Perimetre STRING,
        Nature STRING,
        Date DATE,
        Heures STRING,
        Consommation FLOAT,
        Prevision_J_1 FLOAT,
        Prevision_J FLOAT,
        Fioul FLOAT,
        Charbon FLOAT,
        Gaz FLOAT,
        Nucleaire FLOAT,
        Eolien FLOAT,
        Solaire FLOAT,
        Hydraulique FLOAT,
        Pompage FLOAT,
        Bioenergies FLOAT,
        Echanges_physiques FLOAT,
        Taux_de_CO2 FLOAT,
        ECH_Comm_UK FLOAT,
        ECH_Comm_Espagne FLOAT,
        ECH_Comm_Italie FLOAT,
        ECH_Comm_Suisse FLOAT,
        ECH_Comm_Allemagne_Belgique FLOAT,
        Fioul_TAC FLOAT,
        Fioul_Cogeneration FLOAT,
        Fioul_Autres FLOAT,
        Gaz_TAC FLOAT,
        Gaz_Cogeneration FLOAT,
        Gaz_CCG FLOAT,
        Gaz_Autres FLOAT,
        Hydraulique_Fil_Eau FLOAT,
        Hydraulique_Lacs FLOAT,
        Hydraulique_STEP_Turbinage FLOAT,
        Bio_Dec FLOAT,
        Bio_Biomasse FLOAT,
        Bio_Biogaz FLOAT,
        Stockage_Batterie FLOAT,
        Destockage_Batterie FLOAT,
        Eolien_Terrestre FLOAT,
        Eolien_Offshore FLOAT
    );
    """
    snowflake_hook.run(create_table_sql)

    # Chemin du fichier CSV
    file_path = '/opt/airflow/data/eCO2mix_RTE_En-cours-TR/eCO2mix_RTE_En-cours-TR.csv'
    stage_name = 'RTE_STAGE_ECO2MIX'

    # Vérification du fichier
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Fichier introuvable: {file_path}")

    # Création du stage interne si nécessaire
    snowflake_hook.run(f"CREATE STAGE IF NOT EXISTS {stage_name}")

    # Chargement du fichier dans le stage
    put_command = f"PUT file://{file_path} @{stage_name}"
    snowflake_hook.run(put_command)

    # Copie des données sans spécifier les colonnes
    copy_query = f"""
    COPY INTO eco2mix_data
    FROM @{stage_name}/eCO2mix_RTE_En-cours-TR.csv
    FILE_FORMAT = (
        TYPE = 'CSV',
        FIELD_DELIMITER = ';',
        SKIP_HEADER = 1,
        FIELD_OPTIONALLY_ENCLOSED_BY = '"',
        TRIM_SPACE = TRUE,
        ENCODING = 'ISO-8859-1',
        DATE_FORMAT = 'YYYY-MM-DD',
        NULL_IF = ('NULL', 'null', '')
    )
    ON_ERROR = 'CONTINUE';
    """
    
    # Exécution de la copie
    snowflake_hook.run(copy_query)
    
    # Vérification du chargement
    result = snowflake_hook.get_first("SELECT COUNT(*) FROM eco2mix_data")
    print(f"✅ Données chargées avec succès. Nombre de lignes: {result[0]}")

# Définition du DAG Airflow
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

dag = DAG(
    'upload_eco2_to_snowflake',
    default_args=default_args,
    description='Chargement des données eCO2mix dans Snowflake',
    schedule_interval=None,
    catchup=False
)

upload_task = PythonOperator(
    task_id='upload_to_snowflake',
    python_callable=upload_to_snowflake,
    dag=dag
)

upload_task
