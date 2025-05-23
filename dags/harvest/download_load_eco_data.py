from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.models.baseoperator import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# --- Votre opérateur personnalisé ---
class UploadEcomixToSnowflakeOperator(BaseOperator):
    def __init__(self, file_path: str, table_name: str, stage_name: str, conn_id: str = 'snowflake', **kwargs):
        super().__init__(**kwargs)
        self.file_path = file_path
        self.table_name = table_name
        self.stage_name = stage_name
        self.conn_id = conn_id

    def execute(self, context):
        snowflake_hook = SnowflakeHook(snowflake_conn_id=self.conn_id)
        self.log.info("✅ Connexion établie avec Snowflake")

        conn = snowflake_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
        database, schema = cursor.fetchone()
        cursor.close()

        full_table_name = f'{database}.{schema}.{self.table_name}'
        full_stage_name = f'{database}.{schema}.{self.stage_name}'

        create_table_sql = f"""CREATE OR REPLACE TABLE {full_table_name} (...);"""  # raccourci ici
        snowflake_hook.run(create_table_sql)

        put_command = f"PUT file://{self.file_path} @{full_stage_name} AUTO_COMPRESS=FALSE"
        snowflake_hook.run(put_command)

        copy_query = f"""COPY INTO {full_table_name} FROM @{full_stage_name} FILES = ('eco2mix_data.csv') FILE_FORMAT = (...)"""
        snowflake_hook.run(copy_query)

        self.log.info("✅ Données insérées avec succès dans Snowflake.")

# --- Déclaration du DAG ---
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='upload_eco2mix_to_snowflake',
    default_args=default_args,
    description='Upload eco2mix CSV data to Snowflake',
    schedule_interval=None,  # ou "0 6 * * *" pour une exécution quotidienne à 6h
    start_date=days_ago(1),
    catchup=False,
    tags=['eco2mix', 'snowflake'],
) as dag:

    upload_to_eco_snowflake_task = UploadEcomixToSnowflakeOperator(
        task_id='upload_eco_to_snowflake',
        file_path='/opt/airflow/data/eco2mix_data.csv',
        table_name='ECO2MIX_DATA',
        stage_name='ENEDIS_STAGE',
        conn_id='snowflake',
    )

    upload_to_eco_snowflake_task  # une seule tâche dans le DAG
