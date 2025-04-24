
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

# Fonction d'insertion dans Snowflake
def upload_to_snowflake():
    conn_params = {
        'user': 'HADJIRABK',          # Utilisateur Snowflake
        'password' : '42XCDpmzwMKxRww',
        'account': 'OKVCAFF-IE00559', # Compte Snowflake
        'warehouse': 'COMPUTE_WH', # Entrepôt Snowflake
        'database': 'BRONZE', # Base de données
        'schema': "ENEDIS"      
    }

    # Connexion à Snowflake
    snowflake_hook = SnowflakeHook(
        snowflake_conn_id='snowflake_conn', 
        **conn_params  # Ajouter les paramètres de connexion
    )

    # S'assurer que la base de données et le schéma sont sélectionnés
    snowflake_hook.run(f"USE DATABASE {conn_params['database']}")
    snowflake_hook.run(f"USE SCHEMA {conn_params['schema']}")

    # Créer la table si elle n'existe pas
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS temperature_data (
        Horodate STRING,
        Temp_realisee_lissee_C FLOAT,
        Temp_normale_lissee_C FLOAT,
        Diff_Temp_Realisee_Normale_C FLOAT,
        Pseudo_rayonnement FLOAT,
        Annee INT,
        Mois INT,
        Jour INT,
        Annee_Mois_Jour STRING
    );
    """
    snowflake_hook.run(create_table_sql)

    # Charger le fichier CSV dans le stage interne
    file_path = '/opt/airflow/data/temperature_radiation.csv'
    stage_name = 'ENEDIS_STAGE'

    # Utiliser la commande PUT pour charger le fichier dans le stage
    put_command = f"PUT file://{file_path} @{stage_name}"
    snowflake_hook.run(put_command)
    
    # Copier les données depuis le stage dans la table Snowflake
    copy_query = """
    COPY INTO temperature_data
    FROM @ENEDIS_STAGE/temperature_radiation.csv
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"', FIELD_DELIMITER = ';')
    ON_ERROR = 'CONTINUE';
"""
    snowflake_hook.run(copy_query)

    print("✅ Données insérées avec succès dans Snowflake.")

# Définir le DAG Airflow
dag = DAG(
    'upload_temperature_radiation_to_snowflake',
    description='DAG pour uploader les données de température et de rayonnement dans Snowflake',
    schedule_interval=None,  # Ce DAG ne sera pas planifié, il sera exécuté manuellement
    start_date=datetime(2025, 4, 24),
    catchup=False
)

# Définir la tâche d'exécution Python
upload_task = PythonOperator(
    task_id='upload_to_snowflake',
    python_callable=upload_to_snowflake,
    dag=dag
)

# Assurer que la tâche d'upload s'exécute
upload_task
