from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests
import os
import zipfile
import pandas as pd
from datetime import datetime

class Eco2mixOperator(BaseOperator):
    @apply_defaults
    def __init__(self, start_date, end_date, output_path, *args, **kwargs):
        super(Eco2mixOperator, self).__init__(*args, **kwargs)
        self.start_date = start_date
        self.end_date = end_date
        self.output_path = output_path

    def execute(self, context):
        # URL de l'API pour télécharger les données
        url = f"https://www.rte-france.com/eco2mix/telecharger-les-indicateurs"
        
        # Paramètres de filtrage de la date
        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "type": "csv"
        }

        # Effectuer la requête
        response = requests.get(url, params=params)

        if response.status_code == 200:
            # Sauvegarder le fichier ZIP
            zip_path = os.path.join(self.output_path, f"eco2mix_{self.start_date}_{self.end_date}.zip")
            with open(zip_path, 'wb') as f:
                f.write(response.content)
            self.log.info(f"Fichier ZIP téléchargé : {zip_path}")
        else:
            raise Exception(f"Erreur lors du téléchargement des données: {response.status_code}")

        # Décompresser le fichier ZIP
        extracted_dir = os.path.join(self.output_path, f"eco2mix_{self.start_date}_{self.end_date}")
        os.makedirs(extracted_dir, exist_ok=True)

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extracted_dir)
        self.log.info(f"Fichiers extraits dans : {extracted_dir}")

        # Lire le fichier CSV et le sauvegarder dans le répertoire final
        csv_path = os.path.join(extracted_dir, "data.csv")  # Ajuste le nom selon le contenu de ton ZIP
        df = pd.read_csv(csv_path, encoding='ISO-8859-1', delimiter=';')
        
        final_csv_path = os.path.join(self.output_path, f"eco2mix_{self.start_date}_{self.end_date}.csv")
        df.to_csv(final_csv_path, index=False)
        self.log.info(f"Fichier CSV généré : {final_csv_path}")
