from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import requests
from datetime import datetime
import os
import subprocess
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import csv

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}



def download_csv_file():
    url = "https://data.ademe.fr/api/explore/v2.1/catalog/datasets/prod-region-annuelle-enr/exports/csv"
    local_path = "/opt/airflow/data/prod_region_annuelle_enr.csv"

    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f"Échec du téléchargement : {response.status_code}")
    
    with open(local_path, "wb") as f:
        f.write(response.content)



def load_csv_to_snowflake():
    file_path = "/opt/airflow/data/prod_region_annuelle_enr.csv"
    conn_params = {'user': 'HADJIRA25', 'password' : '42XCDpmzwMKxRww', 'account': 'TRMGRRV-JN45028',
    'warehouse': 'INGESTION_WH', 'database': 'BRONZE',  'schema': "ENEDIS" }
    snowflake_hook = SnowflakeHook( snowflake_conn_id='snowflake_conn', **conn_params)
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    create_table_sql = f"""
        CREATE OR REPLACE TABLE production_region (
            annee INT,
            nom_insee_region STRING,
            code_insee_region STRING,
            production_hydraulique_renouvelable FLOAT,
            production_bioenergies_renouvelable FLOAT,
            production_eolienne_renouvelable FLOAT,
            production_solaire_renouvelable FLOAT,
            production_electrique_renouvelable FLOAT,
            production_gaz_renouvelable FLOAT,
            production_totale_renouvelable FLOAT);"""
    snowflake_hook.run(create_table_sql)
    print("Table crée avec succès dans Snowflake.")
    file_path = '/opt/airflow/data/prod_region_annuelle_enr.csv'
    stage_name = 'RTE_STAGE'
    put_command = f"PUT file://{file_path} @{stage_name}"
    snowflake_hook.run(put_command)
    copy_query = """ COPY INTO production_region FROM @RTE_STAGE/prod_region_annuelle_enr.csv
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"', FIELD_DELIMITER = ';') ON_ERROR = 'CONTINUE'; """
    snowflake_hook.run(copy_query)
    print("Données insérées avec succès dans Snowflake.")

dag = DAG(dag_id="load_enr_csv_to_snowflake",
    start_date=datetime(2014, 1, 1),
    catchup=False,
    default_args=default_args, schedule_interval="@daily",
    tags=["production"],) 

download_task = PythonOperator(task_id="download_csv", python_callable=download_csv_file, dag=dag,)

load_task = PythonOperator(task_id="load_csv_to_snowflake", python_callable=load_csv_to_snowflake, dag=dag,)

download_task >> load_task