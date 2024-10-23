import os
import json
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from dotenv import load_dotenv
from mysql.connector import Error

# Charger les variables d'environnement
load_dotenv()

# Connexion à la base de données MySQL via SQLAlchemy
def connect_to_db():
    try:
        engine = create_engine(f"mysql+mysqlconnector://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME_2')}")
        return engine
    except Error as e:
        print(f"Erreur de connexion à MySQL : {e}")
        return None

# Fonction pour lire le JSON et le transformer en DataFrame
def load_json_to_df(file_path):
    try:
        df = pd.read_json(file_path)
        return df
    except ValueError as e:
        print(f"Erreur lors du chargement du fichier JSON {file_path} : {e}")
        return None

# Fonction pour insérer un DataFrame dans une table de la base de données
def insert_df_to_db(df, table_name, engine):
    try:
        # Insère les données dans la table
        df.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(f"Données insérées dans la table {table_name}.")
    except Exception as e:
        print(f"Erreur lors de l'insertion dans la table {table_name} : {e}")

# Fonction principale
def main():
    # Connexion à la base de données
    engine = connect_to_db()

    if engine is None:
        print("Arrêt du programme : la connexion à la base de données a échoué.")
        return

    # Dossier contenant les fichiers JSON
    directory = './source_json'  # Remplace avec ton chemin réel

    # Fichiers et tables associées
    file_table_mapping = {
        'clients.json': 'clients',
        'ventes.json': 'ventes',
        'produits_sous-categorie.json': 'products'
    }

    # Parcourir les fichiers JSON et insérer les données dans la bonne table
    for file, table_name in file_table_mapping.items():
        file_path = os.path.join(directory, file)
        
        # Charger le JSON en DataFrame
        df = load_json_to_df(file_path)
        
        if df is not None:
            # Insérer le DataFrame dans la base de données
            insert_df_to_db(df, table_name, engine)
        else:
            print(f"Le fichier {file} n'a pas pu être chargé correctement.")

    # Fermer la connexion
    print("Fin du programme et connexion fermée.")

# Exécuter le script principal
if __name__ == "__main__":
    main()
