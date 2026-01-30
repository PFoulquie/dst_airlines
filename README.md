✈️ DST Airlines - Data Pipeline
Projet de pipeline de données automatisé pour le suivi des vols et des infrastructures aéroportuaires. Ce projet utilise une architecture Medallion pour garantir la qualité et l'historisation des données.

🏗️ Architecture des Données
Le projet repose sur une base PostgreSQL distante structurée en deux schémas principaux :

Staging (Bronze) : Réception des données brutes de l'API AirLabs. Les tables sont écrasées à chaque rafraîchissement (Replace).

Acquisition (Silver) : Données nettoyées, dédoublonnées et enrichies.

Les aéroports sont historisés et enrichis avec des données géographiques via OpenStreetMap.

Les vols sont gérés via une logique d'Upsert (Update or Insert) pour conserver l'historique sans doublons.

🛠️ Installation et Configuration
1. Prérequis
Python 3.10+

Un environnement virtuel actif (env)

La bibliothèque python-dotenv pour la gestion des secrets.

2. Installation des dépendances
Bash
pip install -r requirements.txt
3. Configuration de l'environnement
Créez un fichier .env à la racine du projet (ce fichier est ignoré par Git). Utilisez le modèle suivant :

Extrait de code
# API Key AirLabs
AIRLABS_API_KEY=votre_cle_api

# Base de Données PostgreSQL (Serveur Distant)
DB_USER=exploitation
DB_PASSWORD=votre_mot_de_passe
DB_HOST=XX.XX.XX.XX  # Demander l'IP à l'administrateur
DB_PORT=5432
DB_NAME=data_hub
🚀 Utilisation du Pipeline
Exécutez les scripts dans l'ordre suivant pour mettre à jour la base :

Ingestion : python 1_ingestion/ingestion_airlabs.py

Nettoyage Silver : python 2_silver_processing/silver_flights_clean.py

Enrichissement Géo : python 2_silver_processing/silver_airports_geo.py

📈 Évolutions à venir (Roadmap)
[ ] Migration de la couche Bronze vers MongoDB (Docker local).

[ ] Mise en place de dbt pour les transformations SQL.

[ ] Création de la couche Gold pour les indicateurs de performance (KPIs).