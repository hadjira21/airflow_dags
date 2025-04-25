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
        'schema': "RTE"      
    }
    # Connexion à Snowflake
    snowflake_hook = SnowflakeHook(
        snowflake_conn_id='snowflake_conn',
        **conn_params
    )

    # Sélectionner la base de données et le schéma
    snowflake_hook.run(f"USE DATABASE {conn_params['database']}")
    snowflake_hook.run(f"USE SCHEMA {conn_params['schema']}")

    # Créer la table si elle n'existe pas
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS eco2mix_data (
        Perimetre TEXT,
        Nature TEXT,
        Date DATE,
        Heures TEXT,
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
        Echanges_comm_Angleterre FLOAT,
        Echanges_comm_Espagne FLOAT,
        Echanges_comm_Italie FLOAT,
        Echanges_comm_Suisse FLOAT,
        Echanges_comm_Allemagne_Belgique FLOAT,
        Fioul_TAC FLOAT,
        Fioul_Cogen FLOAT,
        Fioul_Autres FLOAT,
        Gaz_TAC FLOAT,
        Gaz_Cogen FLOAT,
        Gaz_CCG FLOAT,
        Gaz_Autres FLOAT,
        Hydraulique_Fil_Eau_Eclusee FLOAT,
        Hydraulique_Lacs FLOAT,
        Hydraulique_STEP_Turbinage FLOAT,
        Bioenergies_Dechets FLOAT,
        Bioenergies_Biomasse FLOAT,
        Bioenergies_Biogaz FLOAT,
        Stockage_Batterie FLOAT,
        Destockage_Batterie FLOAT,
        Eolien_Terrestre FLOAT,
        Eolien_Offshore FLOAT
    );
    """
    snowflake_hook.run(create_table_sql)

    # Charger le fichier CSV dans le stage interne
    file_path = '/opt/airflow/data/eCO2mix_RTE_En-cours-TR/eCO2mix_RTE_En-cours-TR.csv'
    stage_name = 'RTE_STAGE_ECO2MIX'

    # Charger le fichier dans le stage interne
    put_command = f"PUT file://{file_path} @{stage_name}"
    snowflake_hook.run(put_command)

    # Copier les données depuis le stage vers la table Snowflake
    copy_query = """
    COPY INTO eco2mix_data
    FROM @RTE_STAGE_ECO2MIX/eCO2mix_RTE_En-cours-TR/eCO2mix_RTE_En-cours-TR.csv
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"', FIELD_DELIMITER = ';')
    ON_ERROR = 'CONTINUE';
    """
    snowflake_hook.run(copy_query)

    print("✅ Données insérées avec succès dans Snowflake.")

# Définir le DAG Airflow
dag = DAG(
    'upload_eco2_to_snowflake',
    description='DAG pour uploader les données eco2 dans Snowflake',
    schedule_interval=None,  # DAG déclenché manuellement
    start_date=datetime(2025, 4, 24),
    catchup=False
)

# Tâche Python exécutant l'insertion
upload_task = PythonOperator(
    task_id='upload_to_snowflake',
    python_callable=upload_to_snowflake,
    dag=dag
)

upload_task
