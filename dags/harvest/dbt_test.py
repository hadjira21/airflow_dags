from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
}

with DAG(
    dag_id='test_snowflake_connection',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['test', 'snowflake'],
) as dag:

    test_query = SnowflakeOperator(
        task_id='test_snowflake',
        sql='SELECT current_version();',
        snowflake_conn_id='snowflake_default',
    )
