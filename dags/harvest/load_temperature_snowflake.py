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

def load_to_snowflake():
    """Charge le fichier CSV directement dans Snowflake."""
    if not os.path.exists(CSV_FILE):
        raise FileNotFoundError(f"Le fichier CSV n'existe pas : {CSV_FILE}")

    # Snowflake COPY INTO command
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
        database=BRONZE,
        warehouse='COMPUTE_WH',
        schema=ENDIS,
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

# Tâche de chargement des données dans Snowflake
load_task = PythonOperator(
    task_id="load_data_to_snowflake",
    python_callable=load_to_snowflake,
    dag=dag,
)
