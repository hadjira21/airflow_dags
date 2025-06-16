from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import requests
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

API_URL = "https://data.ademe.fr/api/explore/v2.1/catalog/datasets/prod-region-annuelle-enr/records?limit=100"

def create_table():
    conn_params = {'user': 'HADJIRA25', 'password' : '42XCDpmzwMKxRww', 'account': 'TRMGRRV-JN45028',
    'warehouse': 'INGESTION_WH', 'database': 'BRONZE',  'schema': "RTE" }
    hook = SnowflakeHook( snowflake_conn_id='snowflake_conn', **conn_params)
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    cursor = conn.cursor()

    create_sql = """
    CREATE OR REPLACE TABLE STAGING.PROD_REGION_ENR (
        annee INT,
        region STRING,
        filiere STRING,
        valeur FLOAT,
        unite STRING
    );
    """
    cursor.execute(create_sql)
    cursor.close()
    conn.commit()

def extract_api_data(**context):
    response = requests.get(API_URL)
    response.raise_for_status()
    data = response.json()["results"]
    context['ti'].xcom_push(key='enr_data', value=data)

def load_to_snowflake(**context):
    data = context['ti'].xcom_pull(key='enr_data')
    if not data:
        raise ValueError("No data to load")

    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO STAGING.PROD_REGION_ENR (annee, region, filiere, valeur, unite)
        VALUES (%s, %s, %s, %s, %s)
    """

    for row in data:
        cursor.execute(insert_query, (
            row.get("annee"),
            row.get("region"),
            row.get("filiere"),
            row.get("valeur"),
            row.get("unite")
        ))

    cursor.close()
    conn.commit()

with DAG("api_to_snowflake_enr_full",
         default_args=default_args,
         schedule_interval="@daily",
         catchup=False,
         tags=["api", "snowflake"],
         ) as dag:

    create_table_task = PythonOperator(
        task_id="create_table_if_not_exists",
        python_callable=create_table,
    )

    extract_task = PythonOperator(
        task_id="extract_from_api",
        python_callable=extract_api_data,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake,
        provide_context=True,
    )

    create_table_task >> extract_task >> load_task
