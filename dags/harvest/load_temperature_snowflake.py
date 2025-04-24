import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

# Fonction d'insertion dans Snowflake
def upload_to_snowflake():
    # Paramètres de connexion à Snowflake
    conn_params = {
        'user': 'HADJIRABK',          # Utilisateur Snowflake
        'password' : '42XCDpmzwMKxRww',
        'account': 'OKVCAFF-IE00559', # Compte Snowflake
        'warehouse': 'COMPUTE_WH', # Entrepôt Snowflake
        'database': 'BRONZE', # Base de données
        'schema': "ENEDIS"      
    }

    # Lire le fichier CSV avec pandas
    file_path = '/opt/airflow/data/temperature_radiation.csv'
    df = pd.read_csv(file_path, delimiter=';')
    
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
    CREATE TABLE IF NOT EXISTS eco2mix_data (
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

    # Insertion des données dans la table Snowflake
    for _, row in df.iterrows():
        values = tuple(None if pd.isna(v) else v for v in row.tolist())
        placeholders = ', '.join(['%s'] * len(values))  # Nombre de placeholders doit correspondre à celui des colonnes
        insert_query = f"INSERT INTO eco2mix_data VALUES ({placeholders})"
        
        # Exécution de l'insertion
        snowflake_hook.run(insert_query, parameters=values)

    print("✅ Données insérées avec succès dans Snowflake.")

# Définir le DAG Airflow
dag = DAG(
    'upload_temperature_to_snowflake',
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
upload_task
