from airflow.models.baseoperator import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import os


class UploadEcomixToSnowflakeOperator(BaseOperator):
    def __init__(self, file_path: str, table_name: str, stage_name: str, conn_id: str = 'snowflake', **kwargs):
        super().__init__(**kwargs)
        self.file_path = file_path
        self.table_name = table_name
        self.stage_name = stage_name
        self.conn_id = conn_id

    def execute(self, context):
        # Connexion à Snowflake


        conn_params = {
        'user': 'HADJIRABK',
        'password': '42XCDpmzwMKxRww',
        'account': 'OKVCAFF-IE00559',
        'warehouse': 'COMPUTE_WH',
        'database': 'BRONZE',
        'schema': 'METEO' }
        snowflake_hook = SnowflakeHook(
            snowflake_conn_id='snowflake_conn', 
            **conn_params  # Ajouter les paramètres de connexion
        )

        snowflake_hook.run(f"USE DATABASE {conn_params['database']}")
        self.log.info("✅ Connexion établie avec Snowflake")

        # Créer la table si elle n'existe pas
        self.log.info(f"📦 Création de la table {self.table_name} si elle n'existe pas...")
        create_table_sql = f"""
        CREATE OR REPLACE TABLE  {conn_params['database']}.{conn_params['schema']}.{self.table_name} (
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

    # Charger le fichier CSV dans le stage interne
        file_path = '/opt/airflow/data/eco2mix_data.csv'
        stage_name = f'{conn_params['database']}.{conn_params['schema']}.ENEDIS_STAGE'

        # Utiliser la commande PUT pour charger le fichier dans le stage
        put_command = f"PUT file://{file_path} @{stage_name} AUTO_COMPRESS=FALSE"
        snowflake_hook.run(put_command)
        
        # Copier les données depuis le stage dans la table Snowflake
        copy_query = f"""
COPY INTO "BRONZE"."ENEDIS"."ECO2MIX_DATA"
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41
    FROM '@"BRONZE"."ENEDIS"."ENEDIS_STAGE"'
)
FILES = ('eco2mix_data.csv')
FILE_FORMAT = (
    TYPE = CSV,
    SKIP_HEADER = 1,
    FIELD_DELIMITER = ',',
    TRIM_SPACE = TRUE,
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    REPLACE_INVALID_CHARACTERS = TRUE,
    DATE_FORMAT = AUTO,
    TIME_FORMAT = AUTO,
    TIMESTAMP_FORMAT = AUTO
)
FORCE = TRUE
ON_ERROR=ABORT_STATEMENT;
    """
        snowflake_hook.run(copy_query)

        print("✅ Données insérées avec succès dans Snowflake.")
