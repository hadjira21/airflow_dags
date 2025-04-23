from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import os
import requests
import pandas as pd
from io import BytesIO

class Eco2mixDownloadOperator(BaseOperator):

    @apply_defaults
    def __init__(self, output_path, start_date=None, end_date=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output_path = output_path
        self.start_date = start_date
        self.end_date = end_date

    def execute(self, context):
        url = "https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_En-cours-TR.zip"
        self.log.info(f"Téléchargement des données depuis {url}")
        response = requests.get(url)

        if response.status_code != 200:
            raise Exception(f"Échec du téléchargement : {response.status_code}")

        # Sauvegarde temporaire du fichier ZIP en mémoire
        zip_data = BytesIO(response.content)

        # Lecture et transformation du fichier Excel
        with pd.ExcelFile(zip_data) as xls:
            df = pd.read_excel(xls, sheet_name=0)

        if self.start_date and self.end_date:
            df['Date'] = pd.to_datetime(df['Date'])
            df = df[(df['Date'] >= self.start_date) & (df['Date'] <= self.end_date)]

        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        df.to_csv(self.output_path, index=False)
        self.log.info(f"Fichier sauvegardé à {self.output_path}")
