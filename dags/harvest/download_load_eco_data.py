from airflow.models.baseoperator import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


class UploadEcomixToSnowflakeOperator(BaseOperator):
    def __init__(self, file_path: str, table_name: str, stage_name: str, conn_id: str = 'snowflake', **kwargs):
        super().__init__(**kwargs)
        self.file_path = file_path
        self.table_name = table_name
        self.stage_name = stage_name
        self.conn_id = conn_id

    def execute(self, context):
        # Connexion à Snowflake
        snowflake_hook = SnowflakeHook(snowflake_conn_id=self.conn_id)
        self.log.info("✅ Connexion établie avec Snowflake")

        # Utiliser la base de données et le schéma depuis la connexion
        conn = snowflake_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
        database, schema = cursor.fetchone()
        cursor.close()

        full_table_name = f'{database}.{schema}.{self.table_name}'
        full_stage_name = f'{database}.{schema}.{self.stage_name}'

        # Créer la table si elle n'existe pas
        self.log.info(f"📦 Création de la table {full_table_name} si elle n'existe pas...")
        create_table_sql = f"""
        CREATE OR REPLACE TABLE {full_table_name} (
            "Périmètre" VARCHAR,
            "Nature" VARCHAR,
            "Date" VARCHAR,
            "Heures" VARCHAR,
            "Consommation" VARCHAR,
            "Prévision J-1" VARCHAR,
            "Prévision J" VARCHAR,
            "Fioul" VARCHAR,
            "Charbon" VARCHAR,
            "Gaz" VARCHAR,
            "Nucléaire" VARCHAR,
            "Eolien" VARCHAR,
            "Solaire" VARCHAR,
            "Hydraulique" VARCHAR,
            "Pompage" VARCHAR,
            "Bioénergies" VARCHAR,
            "Ech. physiques" VARCHAR,
            "Taux de Co2" VARCHAR,
            "Ech. comm. Angleterre" VARCHAR,
            "Ech. comm. Espagne" VARCHAR,
            "Ech. comm. Italie" VARCHAR,
            "Ech. comm. Suisse" VARCHAR,
            "Ech. comm. Allemagne-Belgique" VARCHAR,
            "Fioul - TAC" VARCHAR,
            "Fioul - Cogén." VARCHAR,
            "Fioul - Autres" VARCHAR,
            "Gaz - TAC" VARCHAR,
            "Gaz - Cogén." VARCHAR,
            "Gaz - CCG" VARCHAR,
            "Gaz - Autres" VARCHAR,
            "Hydraulique - Fil de l?eau + éclusée" VARCHAR,
            "Hydraulique - Lacs" VARCHAR,
            "Hydraulique - STEP turbinage" VARCHAR,
            "Bioénergies - Déchets" VARCHAR,
            "Bioénergies - Biomasse" VARCHAR,
            "Bioénergies - Biogaz" VARCHAR,
            "Stockage batterie" VARCHAR,
            "Déstockage batterie" VARCHAR,
            "Eolien terrestre" VARCHAR,
            "Eolien offshore" VARCHAR,
            "Consommation corrigée" VARCHAR
        );
        """
        snowflake_hook.run(create_table_sql)

        # Upload dans le stage
        self.log.info(f"📤 Upload du fichier CSV dans le stage {full_stage_name}")
        put_command = f"PUT file://{self.file_path} @{full_stage_name} AUTO_COMPRESS=FALSE"
        snowflake_hook.run(put_command)

        # Copy dans la table
        self.log.info("📥 Insertion des données depuis le stage vers la table Snowflake...")
        copy_query = f"""
        COPY INTO {full_table_name}
        FROM (
            SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, 
                   $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, 
                   $39, $40, $41
            FROM @{full_stage_name}
        )
        FILES = ('eco2mix_data.csv')
        FILE_FORMAT = (
            TYPE = 'CSV',
            SKIP_HEADER = 1,
            FIELD_DELIMITER = ',',
            TRIM_SPACE = TRUE,
            FIELD_OPTIONALLY_ENCLOSED_BY = '"',
            REPLACE_INVALID_CHARACTERS = TRUE
        )
        FORCE = TRUE
        ON_ERROR = 'ABORT_STATEMENT';
        """
        snowflake_hook.run(copy_query)

        self.log.info("✅ Données insérées avec succès dans Snowflake.")
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='upload_eco2mix_to_snowflake2',
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
