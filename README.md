# ✈️ DST Airlines - Data Pipeline (ELT Edition)

Projet de pipeline de données automatisé pour le suivi des vols. Le projet a été migré d'une structure de scripts isolés vers une architecture **ELT (Extract-Load-Transform)** pilotée par **Airflow** et **dbt**.

## 🏗️ Architecture des Données

Le projet utilise l'architecture **Medallion** sur PostgreSQL (Supabase) :

* **Bronze (Schema: `bronze`)** : Ingestion des données brutes au format JSON depuis l'API **Air France-KLM**.
* **Silver (Schema: `silver`)** : 
    * `s_flights` : Nettoyage, typage et structuration des données de vols.
    * `s_airports` : Dimension de référence extraite dynamiquement des données de vols (Codes IATA, noms, villes et coordonnées GPS).
* **Gold (Schema: `gold`)** : Couche de présentation pour le reporting et les KPIs.



## 🛠️ Stack Technique

* **Source** : API Air France-KLM (Open Data).
* **Orchestration** : Airflow (Scripts d'ingestion Bronze).
* **Transformation** : dbt (Data Build Tool) pour le modeling SQL.
* **Stockage** : PostgreSQL (Supabase).

## 🚀 Installation et Configuration

### 1. Prérequis
* Python 3.10+
* Un environnement virtuel actif (`venv`)
* Accès SSH configuré pour GitHub

### 2. Configuration de l'environnement
Créez un fichier `.env` à la racine du projet :
```env
# API Key Air France-KLM
AF_API_KEY=votre_cle_api

# Base de Données PostgreSQL
DB_USER=postgres
DB_PASSWORD=votre_mot_de_passe
DB_HOST=votre_host_supabase
DB_PORT=5432
DB_NAME=postgres
DB_SCHEMA=silver