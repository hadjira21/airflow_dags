from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

# Dossier où les fichiers CSV sont déjà présents
DATA_DIR = "/opt/airflow/data"
CSV_FILE = os.path.join(DATA_DIR, "temperature_radiation.csv")
SNOWFLAKE_DATABASE = "BRONZE"
SNOWFLAKE_SCHEMA = "ENEDIS"
SNOWFLAKE_TABLE = "temperature_data"

def create_table_if_not_exists():
    """Crée la table dans Snowflake si elle n'existe pas déjà."""
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
        date DATE,
        temperature DECIMAL,
        radiation DECIMAL
    );
    """
    
    # Utilisation de l'opérateur Snowflake pour créer la table dans Snowflake
    create_table_task = SnowflakeOperator(
        task_id='create_table_if_not_exists',
        sql=create_table_sql,
        snowflake_conn_id='snowflake_conn',
        database=SNOWFLAKE_DATABASE,
        warehouse='COMPUTE_WH',
        schema=SNOWFLAKE_SCHEMA,
        autocommit=True,
    )
    
    # Exécution de la tâche
    create_table_task.execute()

def load_to_snowflake():
    """Charge le fichier CSV directement dans Snowflake."""
    if not os.path.exists(CSV_FILE):
        raise FileNotFoundError(f"Le fichier CSV n'existe pas : {CSV_FILE}")

    # Commande COPY INTO pour charger le fichier CSV dans Snowflake
    sql = f"""
    COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}
    FROM @~/temperature_radiation.csv
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"')
    """
    
    # Utilisation de l'opérateur Snowflake pour charger les données dans Snowflake
    snowflake_task = SnowflakeOperator(
        task_id='load_data_to_snowflake',
        sql=sql,
        snowflake_conn_id='snowflake_conn',
        database=SNOWFLAKE_DATABASE,
        warehouse='COMPUTE_WH',
        schema=SNOWFLAKE_SCHEMA,
        autocommit=True,
    )
    
    # Exécution de la tâche
    snowflake_task.execute()

# Définition du DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 20),
    "retries": 0,
}

dag = DAG(
    "load_temperature_data_to_snowflake",
    default_args=default_args,
    schedule_interval="@daily",  # Exécution quotidienne
    catchup=False,
)

# Tâches dans le DAG
create_table_task = PythonOperator(
    task_id="create_table_if_not_exists",
    python_callable=create_table_if_not_exists,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_data_to_snowflake",
    python_callable=load_to_snowflake,
    dag=dag,
)

# Ordre d'exécution des tâches : créer la table d'abord, puis charger les données
create_table_task >> load_task
