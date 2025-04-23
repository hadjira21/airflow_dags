from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests
import zipfile
import io
import os
from datetime import datetime

class Eco2mixOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        url: str,
        output_path: str,
        start_date: str = None,
        end_date: str = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.url = url
        self.output_path = output_path
        self.start_date = start_date
        self.end_date = end_date

    def execute(self, context):
        self.log.info("Téléchargement des données depuis %s", self.url)

        response = requests.get(self.url)
        if response.status_code != 200:
            raise Exception(f"Erreur de téléchargement : {response.status_code}")

        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            for filename in z.namelist():
                if filename.endswith(".csv"):
                    self.log.info("Extraction du fichier : %s", filename)
                    with z.open(filename) as f:
                        full_output_path = os.path.join(self.output_path, filename)
                        os.makedirs(self.output_path, exist_ok=True)
                        with open(full_output_path, "wb") as out_file:
                            out_file.write(f.read())
                    self.log.info("Fichier stocké à : %s", full_output_path)

        self.log.info("Téléchargement terminé.")
