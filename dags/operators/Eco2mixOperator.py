from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests

class Eco2mixOperator(BaseOperator):

    @apply_defaults
    def __init__(self, url: str, output_path: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = url
        self.output_path = output_path

    def execute(self, context):
        self.log.info(f"Téléchargement depuis {self.url}")
        response = requests.get(self.url)
        if response.status_code == 200:
            with open(self.output_path, 'wb') as f:
                f.write(response.content)
            self.log.info(f"Fichier sauvegardé à : {self.output_path}")
        else:
            raise Exception(f"Erreur lors du téléchargement : {response.status_code}")


