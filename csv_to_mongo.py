import os
import pandas as pd
import glob
from pymongo import MongoClient
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Charger les informations de connexion MongoDB à partir des variables d'environnement
MONGO_URI = os.getenv('MONGO_URI')
DATABASE_NAME = os.getenv('MONGO_DB_NAME')

# Connexion à MongoDB
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]

# Test de la connexion à MongoDB
try:
    # Accéder à une collection spécifique pour vérifier la connexion
    db.list_collection_names()
    print(f"Connexion réussie à la base de données MongoDB '{DATABASE_NAME}'")
except Exception as e:
    print(f"Erreur de connexion à MongoDB: {e}")
    exit(1)

# Chemin du dossier contenant les fichiers CSV
folder_path = './source_csv'

# Parcourir tous les fichiers CSV dans le dossier
for csv_file in glob.glob(f"{folder_path}/*.csv"):
    try:
        # Charger chaque fichier CSV dans un DataFrame
        df = pd.read_csv(csv_file, sep=";")
        
        # Convertir le DataFrame en une liste de dictionnaires (chaque ligne devient un document)
        data = df.to_dict(orient='records')
        
        # Déterminer le nom de la collection basé sur le nom du fichier (sans l'extension)
        collection_name = os.path.splitext(os.path.basename(csv_file))[0]
        
        # Insérer les données dans la collection MongoDB
        db[collection_name].insert_many(data)

        print(f"Les données de {csv_file} ont été insérées dans la collection '{collection_name}' avec succès.")
    
    except Exception as e:
        print(f"Erreur lors de l'insertion des données de {csv_file} dans MongoDB : {e}")

# Fermer la connexion à MongoDB
client.close()
print("Connexion à MongoDB fermée.")
