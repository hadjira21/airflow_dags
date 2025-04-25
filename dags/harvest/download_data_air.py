from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import requests
import pandas as pd
import json
from io import StringIO

# Configuration
API_URL = "https://opendata.atmosud.org/api/v1/"
SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'BRONZE'
SNOWFLAKE_SCHEMA = 'ATMOSUD'
SNOWFLAKE_TABLE = 'ATMOSUD_DATA'
SNOWFLAKE_STAGE = 'ATMOSUD_STAGE'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def fetch_atmosud_data():
    """Récupère les données depuis l'API AtmoSud"""
    endpoint = "mesures"
    params = {
        'size': 10000,  # Nombre de résultats à retourner
        'page': 1,
        'sort': 'date_debut,asc'
    }
    
    try:
        response = requests.get(f"{API_URL}{endpoint}", params=params)
        response.raise_for_status()
        
        data = response.json()
        print(f"Récupéré {len(data.get('data', []))} enregistrements")
        
        # Convertir en DataFrame
        df = pd.json_normalize(data['data'])
        
        # Sauvegarder en CSV temporaire
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_content = csv_buffer.getvalue()
        
        return csv_content
        
    except Exception as e:
        print(f"Erreur lors de la récupération des données: {str(e)}")
        raise

def process_and_upload(**kwargs):
    """Traite et charge les données dans Snowflake"""
    ti = kwargs['ti']
    csv_content = ti.xcom_pull(task_ids='fetch_atmosud_data')
    
    if not csv_content:
        raise ValueError("Aucune donnée récupérée depuis l'API")
    
    # Charger dans DataFrame pour traitement
    df = pd.read_csv(StringIO(csv_content))
    
    # Nettoyage des données
    df.columns = [col.upper().replace('.', '_') for col in df.columns]
    
    # Connexion Snowflake
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    try:
        # Créer la table si elle n'existe pas (schéma dynamique)
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
            {', '.join([f"{col} STRING" for col in df.columns])}
        )
        """
        cursor.execute(create_table_sql)
        
        # Créer le stage
        cursor.execute(f"CREATE STAGE IF NOT EXISTS {SNOWFLAKE_STAGE}")
        
        # Upload des données
        temp_file = '/tmp/atmosud_data.csv'
        df.to_csv(temp_file, index=False)
        
        put_sql = f"PUT file://{temp_file} @{SNOWFLAKE_STAGE}"
        cursor.execute(put_sql)
        
        # Copie des données
        copy_sql = f"""
        COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}
        FROM @{SNOWFLAKE_STAGE}/atmosud_data.csv
        FILE_FORMAT = (
            TYPE = CSV
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            NULL_IF = ('NULL', 'null', '')
        )
        """
        cursor.execute(copy_sql)
        
        # Vérification
        cursor.execute(f"SELECT COUNT(*) FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}")
        count = cursor.fetchone()[0]
        print(f"✅ {count} enregistrements chargés avec succès")
        
    except Exception as e:
        print(f"❌ Erreur Snowflake: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()
        import os
        if os.path.exists(temp_file):
            os.remove(temp_file)

# Définition du DAG
dag = DAG(
    'atmosud_air_quality_pipeline',
    default_args=default_args,
    description='Récupère les données de qualité de l\'air depuis AtmoSud',
    schedule_interval='@daily',
    catchup=False
)

# Tâches
fetch_data_task = PythonOperator(
    task_id='fetch_atmosud_data',
    python_callable=fetch_atmosud_data,
    dag=dag
)

process_upload_task = PythonOperator(
    task_id='process_and_upload',
    python_callable=process_and_upload,
    provide_context=True,
    dag=dag
)

# Orchestration
fetch_data_task >> process_upload_task
