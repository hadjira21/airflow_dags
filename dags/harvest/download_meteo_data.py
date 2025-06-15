import gzip  
import shutil
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import subprocess
import pandas as pd
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook # type: ignore

DATA_DIR = "/opt/airflow/data"
GZ_FILE = os.path.join(DATA_DIR, "meteo.csv.gz")
CSV_FILE = os.path.join(DATA_DIR, "meteo.csv") 

def download_data():
    os.makedirs(DATA_DIR, exist_ok=True)
    url = "https://www.data.gouv.fr/fr/datasets/r/c1265c02-3a8e-4a28-961e-26b2fd704fe8"  
    command = ["curl", "-L", "-o", GZ_FILE, url]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Fichier téléchargé avec succès : {GZ_FILE}")
    else:
        raise Exception(f"Erreur lors du téléchargement : {result.stderr}")

def decompress_file():
    if not os.path.exists(GZ_FILE):
        raise FileNotFoundError(f"Le fichier .gz n'existe pas : {GZ_FILE}")
    try:
        with gzip.open(GZ_FILE, 'rb') as f_in:
            with open(CSV_FILE, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print(f"Fichier décompressé avec succès : {CSV_FILE}")
    except Exception as e:
        raise Exception(f"Erreur lors de la décompression du fichier .gz : {e}")

# def read_data():
#     """Lit le fichier CSV décompressé et affiche un aperçu."""
#     if not os.path.exists(CSV_FILE):
#         raise FileNotFoundError(f"Le fichier CSV n'existe pas : {CSV_FILE}")

#     try:
#         df = pd.read_csv(CSV_FILE, encoding='ISO-8859-1', delimiter=';')
#         print("Aperçu des données :")
#         print(df.head())
#     except Exception as e:
#         print(f"Erreur lors de la lecture du fichier CSV : {e}")
# def upload_to_snowflake():
#     conn_params = {
#         'user': 'HADJIRA25',  
#         'password' : '42XCDpmzwMKxRww',
#         'account': 'TRMGRRV-JN45028',
#         'warehouse': 'COMPUTE_WH', 
#         'database': 'BRONZE', 
#         'schema': "METEO"      
#     }
#     # OKVCAFF-IE00559 
#     # Connexion à Snowflake
#     snowflake_hook = SnowflakeHook(
#         snowflake_conn_id='snowflake_conn', 
#         **conn_params 
#     )
#     snowflake_hook.run(f"USE DATABASE {conn_params['database']}")
#     snowflake_hook.run(f"USE SCHEMA {conn_params['schema']}")
  
#     # Créer la table si elle n'existe pas
#     create_table_sql = """ CREATE TABLE IF NOT EXISTS meteo_data (
#     NUM_POSTE VARCHAR,
#     NOM_USUEL VARCHAR,
#     LAT FLOAT,
#     LON FLOAT,
#     ALTI FLOAT,
#     AAAAMMJJ DATE,
#     RR FLOAT,
#     QRR INTEGER,
#     TN FLOAT,
#     QTN INTEGER,
#     HTN TIME,
#     QHTN INTEGER,
#     TX FLOAT,
#     QTX INTEGER,
#     HTX TIME,
#     QHTX INTEGER,
#     TM FLOAT,
#     QTM INTEGER,
#     TNTXM FLOAT,
#     QTNTXM INTEGER,
#     TAMPLI FLOAT,
#     QTAMPLI INTEGER,
#     TNSOL FLOAT,
#     QTNSOL INTEGER,
#     TN50 FLOAT,
#     QTN50 INTEGER,
#     DG FLOAT,
#     QDG INTEGER,
#     FFM FLOAT,
#     QFFM INTEGER,
#     FF2M FLOAT,
#     QFF2M INTEGER,
#     FXY FLOAT,
#     QFXY INTEGER,
#     DXY FLOAT,
#     QDXY INTEGER,
#     HXY TIME,
#     QHXY INTEGER,
#     FXI FLOAT,
#     QFXI INTEGER,
#     DXI FLOAT,
#     QDXI INTEGER,
#     HXI TIME,
#     QHXI INTEGER,
#     FXI2 FLOAT,
#     QFXI2 INTEGER,
#     DXI2 FLOAT,
#     QDXI2 INTEGER,
#     HXI2 TIME,
#     QHXI2 INTEGER,
#     FXI3S FLOAT,
#     QFXI3S INTEGER,
#     DXI3S FLOAT,
#     QDXI3S INTEGER,
#     HXI3S TIME,
#     QHXI3S INTEGER,
#     DRR FLOAT,
#     QDRR INTEGER
# );
#     """
#     snowflake_hook.run(create_table_sql)

#     # Charger le fichier CSV dans le stage interne
#     file_path = '/opt/airflow/data/meteo.csv'
#     stage_name = 'METEO_STAGE'

#     # Utiliser la commande PUT pour charger le fichier dans le stage
#     put_command = f"PUT file://{file_path} @{stage_name}"
#     snowflake_hook.run(put_command)
    
#     # Copier les données depuis le stage dans la table Snowflake
#     copy_query = """
#     COPY INTO meteo_data
#     FROM @METEO_STAGE/meteo.csv
#     FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"', FIELD_DELIMITER = ';')
#     ON_ERROR = 'CONTINUE';
# """
#     snowflake_hook.run(copy_query)

#     print("Données insérées avec succès dans Snowflake.")
def upload_to_snowflake():
    import pandas as pd
    from snowflake.connector.pandas_tools import write_pandas

    conn_params = {
        'user': 'HADJIRA25',
        'password': '42XCDpmzwMKxRww',
        'account': 'TRMGRRV-JN45028',
        'warehouse': 'COMPUTE_WH',
        'database': 'BRONZE',
        'schema': "METEO"
    }

    # Lire les données depuis le CSV
    file_path = '/opt/airflow/data/meteo.csv'
    df = pd.read_csv(file_path, delimiter=';', encoding='ISO-8859-1')

    # Connexion à Snowflake via hook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn', **conn_params)
    engine = hook.get_sqlalchemy_engine()

    # Créer une table et insérer automatiquement
    success, nchunks, nrows, _ = write_pandas(
        conn=engine.raw_connection(),
        df=df,
        table_name="meteo_data",
        schema=conn_params["schema"],
        database=conn_params["database"],
        auto_create_table=True,
        quote_identifiers=True
    )

    print(f"Données chargées dans Snowflake : {nrows} lignes")

default_args = { "owner": "airflow",  "start_date": datetime(2024, 3, 20), "retries": 0,}

dag = DAG( "download_meteo_gz_csv_data", default_args=default_args, schedule_interval="@daily", catchup=False,)
download_task = PythonOperator(task_id="download_gz_csv", python_callable=download_data, dag=dag,)
decompress_task = PythonOperator(task_id="decompress_gz_csv", python_callable=decompress_file, dag=dag,)
read_task = PythonOperator(task_id="read_gz_csv", python_callable=read_data, dag=dag,)
upload_task = PythonOperator(task_id='upload_to_snowflake', python_callable=upload_to_snowflake, dag=dag)
download_task >> decompress_task >> read_task >> upload_task
