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
        'schema': "RTE"      
    }
    
    snowflake_hook = SnowflakeHook(
        snowflake_conn_id='snowflake_conn',
        **conn_params
    )

    # 1. Vérification du fichier CSV
    file_path = '/opt/airflow/data/eCO2mix_RTE_En-cours-TR/eCO2mix_RTE_En-cours-TR.csv'
    try:
        df = pd.read_csv(file_path, delimiter=';')
        print("Aperçu des données CSV:")
        print(df.head())
        print(f"Nombre de lignes dans le CSV: {len(df)}")
        
        # Sauvegarder en CSV avec formatage correct pour Snowflake
        temp_file = '/tmp/eco2mix_formatted.csv'
        df.to_csv(temp_file, sep=';', index=False, encoding='utf-8')
        print(f"Fichier temporaire créé: {temp_file}")
    except Exception as e:
        print(f"❌ Erreur lors de la lecture du CSV: {str(e)}")
        raise

    # 2. Configuration Snowflake
    snowflake_hook.run(f"USE DATABASE {conn_params['database']}")
    snowflake_hook.run(f"USE SCHEMA {conn_params['schema']}")

    # 3. Création de la table (identique à votre version)
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

    # 4. Chargement dans le stage
    stage_name = 'RTE_STAGE_ECO2MIX'
    
    # Créer le stage si nécessaire
    snowflake_hook.run(f"CREATE STAGE IF NOT EXISTS {stage_name}")
    
    # Charger le fichier reformaté
    put_command = f"PUT file://{temp_file} @{stage_name}"
    snowflake_hook.run(put_command)
    
    # Vérification du stage
    list_stage = f"LIST @{stage_name}"
    snowflake_hook.run(list_stage)

    # 5. Copie des données avec paramètres précis
copy_query = """COPY INTO eco2mix_data
FROM (
    SELECT 
        $1::STRING, $2::STRING, 
        TO_DATE($3, 'DD/MM/YYYY'), 
        $4::STRING,
        TRY_CAST(REPLACE($5, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($6, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($7, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($8, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($9, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($10, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($11, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($12, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($13, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($14, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($15, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($16, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($17, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($18, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($19, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($20, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($21, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($22, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($23, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($24, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($25, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($26, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($27, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($28, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($29, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($30, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($31, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($32, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($33, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($34, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($35, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($36, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($37, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($38, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($39, ',', '.') AS FLOAT),
        TRY_CAST(REPLACE($40, ',', '.') AS FLOAT)
    FROM @RTE_STAGE_ECO2MIX/eco2mix_formatted.csv
    )
    FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ';' SKIP_HEADER = 1)
    """
    snowflake_hook.run(copy_query)

    # 6. Vérification des données importées
    count_query = "SELECT COUNT(*) FROM eco2mix_data"
    result = snowflake_hook.get_first(count_query)
    print(f"✅ Données insérées avec succès. Nombre de lignes dans la table: {result[0]}")
    
    # Nettoyage du fichier temporaire
    import os
    os.remove(temp_file)
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
