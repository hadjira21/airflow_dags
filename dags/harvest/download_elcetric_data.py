from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook # type: ignore
from datetime import datetime
import subprocess
import pandas as pd
import os
import pandas as pd
import os
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
DATA_DIR = "/opt/airflow/data"
CSV_FILE = os.path.join(DATA_DIR, "electric_data.csv")

def download_data():
    os.makedirs(DATA_DIR, exist_ok=True)
    url = "https://data.enedis.fr/api/explore/v2.1/catalog/datasets/production-electrique-par-filiere-a-la-maille-commune/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B"
    command = ["curl", "-L", "-o", CSV_FILE, url]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Fichier téléchargé avec succès : {CSV_FILE}")
    else:
        raise Exception(f"Erreur lors du téléchargement : {result.stderr}")

def read_data():
    if not os.path.exists(CSV_FILE):
        raise FileNotFoundError(f"Le fichier CSV n'existe pas : {CSV_FILE}")
    try:
        df = pd.read_csv(CSV_FILE, encoding='ISO-8859-1', delimiter=';')
        print("Aperçu des données :")
        print(df.head())
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier CSV : {e}")


def upload_to_snowflake():
    # Connexion
    conn_params = {'user': 'HADJIRA25', 'password': '42XCDpmzwMKxRww', 'account': 'TRMGRRV-JN45028', 'warehouse': 'COMPUTE_WH', 'database': 'BRONZE', 'schema': 'ENEDIS'}
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn', **conn_params)

    CSV_FILE = "/opt/airflow/data/electric_data.csv"
    table_name = 'electric_data'
    df = pd.read_csv(CSV_FILE, encoding='ISO-8859-1', delimiter=';', low_memory=False)
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn', **conn_params)
    engine = hook.get_sqlalchemy_engine()

    # Création automatique de la table et insertion via write_pandas
    success, nchunks, nrows, _ = write_pandas(
    conn=engine.raw_connection(), df=df, table_name="electric_data", schema=conn_params["schema"],
    database=conn_params["database"], auto_create_table=True, quote_identifiers=True )

    print(f" Données insérées dans Snowflake : {nrows} lignes")
    # # Générer dynamiquement les types SQL à partir de df.dtypes
    # dtype_mapping = {  'object': 'VARCHAR', 'float64': 'FLOAT', 'int64': 'INT',  'bool': 'BOOLEAN', 'datetime64[ns]': 'TIMESTAMP' }

    # columns_sql = []
    # for col in df.columns:
    #     col_type = dtype_mapping.get(str(df[col].dtype), 'VARCHAR')
    #     safe_col = col.replace(" ", "_").replace("-", "_").upper()
    #     columns_sql.append(f'"{safe_col}" {col_type}')

    # create_sql = f'CREATE TABLE IF NOT EXISTS {table_name} ({", ".join(columns_sql)});'

    # conn = snowflake_hook.get_conn()
    # cursor = conn.cursor()
    # try:
    #     cursor.execute(create_sql)
    #     print(f"Table `{table_name}` créée avec succès.")
    # finally:
    #     cursor.close()
    # df.columns = [col.replace(" ", "_").replace("-", "_").upper() for col in df.columns]
    # rows = df.where(pd.notnull(df), None).values.tolist()
    # snowflake_hook.insert_rows(table=table_name.upper(), rows=rows, commit_every=1000)
    # print("Upload vers Snowflake terminé avec succès.")




default_args = { "owner": "airflow", "start_date": datetime(2025, 3, 20), "retries": 0,}
dag = DAG( "download_and_process_electrique_data", default_args=default_args, schedule_interval="@daily", catchup=False,)
download_task = PythonOperator( task_id="download_csv_data", python_callable=download_data, dag=dag, )
read_task = PythonOperator(task_id="read_csv_data", python_callable=read_data, dag=dag,)
upload_task = PythonOperator(task_id='upload_to_snowflake', python_callable=upload_to_snowflake, dag=dag)
download_task >> read_task >> upload_task
