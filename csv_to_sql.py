from sqlalchemy import create_engine
import pandas as pd
import os
import glob
from dotenv import load_dotenv

#charger les variables d'environnement 

load_dotenv()

DATABASE_TYPE = os.getenv('DB_TYPE')
DBAPI = os.getenv('DB_API')
HOST = os.getenv('DB_HOST')
USER = os.getenv('DB_USER')
PASSWORD = os.getenv('DB_PASSWORD')
DATABASE = os.getenv('DB_NAME')
PORT = os.getenv('DB_PORT')

connection_string = f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
engine = create_engine(connection_string)

# Test de la connexion
try:
    with engine.connect() as connection:
        print("Connexion réussie à la base de données !")
except Exception as e:
    print("Erreur de connexion :", e)


#chemin csv

folder_path = './source_csv'


# recup les fichiers csv
for csv_file in glob.glob(f"{folder_path}/*.csv"):
    # Charger chaque fichier CSV dans un DataFrame
    df = pd.read_csv(csv_file, sep=";")
    
    # Déterminer le nom de la table (par exemple, basé sur le nom du fichier)
    table_name = os.path.splitext(os.path.basename(csv_file))[0]
    
    # Insérer les données dans la base de données
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    
    print(f"Les données de {csv_file} ont été insérées dans la table '{table_name}'")
