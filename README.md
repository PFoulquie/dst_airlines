# DST Airlines : End-to-End Data Engineering Pipeline // archi backup

## Présentation du projet
Ce projet implémente un pipeline de données ELT (Extract, Load, Transform) complet et automatisé pour le monitoring du trafic aérien d'Air France-KLM. 

L'infrastructure repose sur une **architecture orientée microservices** entièrement conteneurisée via Docker, garantissant scalabilité, isolation des environnements et reproductibilité.

## Principes d'Architecture & Microservices : Le découpage des conteneurs

Dans une approche monolithique débutante, un seul fichier `Dockerfile` et un seul `requirements.txt` gèrent l'ensemble de l'application. Pour ce projet, nous avons opté pour une approche **microservices stricte**, ce qui explique la présence de fichiers de configuration dédiés (`Dockerfile.api`, `Dockerfile.streamlit`, `requirements.api.txt`, `requirements.streamlit.txt`).

**Ce découpage répond à 4 bonnes pratiques de l'industrie (Best Practices) :**

1. **Séparation des préoccupations (Separation of Concerns) :** Le frontend a pour unique rôle de générer l'interface graphique (IHM). L'API, elle, gère la sécurité, la logique métier et la connexion au Data Warehouse. 
2. **Prévention des conflits (Dependency Hell) :** L'API nécessite des drivers de base de données stricts (`SQLAlchemy`, `psycopg2`), tandis que Streamlit a besoin de librairies de dataviz (`Plotly`, `Pandas`). Les isoler garantit qu'une mise à jour des graphiques du dashboard ne fera jamais crasher la connexion à la base de données.
3. **Optimisation des builds Docker (Layer Caching) :** Grâce à des `Dockerfile` séparés, si nous modifions le code du frontend, Docker ne reconstruira *que* l'image de Streamlit. L'image de l'API reste intacte, ce qui accélère drastiquement les cycles de développement et les futurs pipelines CI/CD.
4. **Sécurité et réduction de la surface d'attaque :** Nos images reposent sur des versions Python minimalistes (`slim`). Le conteneur de l'API, qui a les accès sensibles à Supabase, n'embarque aucune librairie graphique superflue qui pourrait présenter des failles de sécurité.

## Structure du Dépôt

.
├── airflow/                 # DAGs, scripts d'ingestion (API vers Bronze)
├── dbt/                     # Modèles de transformation SQL (Bronze ➔ Silver)
├── docs/                    # Documentation annexe (ex: INSTALL.md)
├── scripts/                 
│   ├── start_pipeline.bat   # Automatisation du lancement (Environnement local)
│   └── stop_pipeline.bat    # Arrêt propre des conteneurs
├── api_app.py               # Code source du Backend (FastAPI)
├── dashboard_afklm.py       # Code source du Frontend (Streamlit)
├── docker-compose.yml       # Orchestration globale des services
├── Dockerfile.api           # Instructions de build - Backend
├── Dockerfile.streamlit     # Instructions de build - Frontend
├── requirements.api.txt     # Dépendances Backend
└── requirements.streamlit.txt # Dépendances Frontend


## Le Pipeline de Données (ELT)

Notre flux de données garantit l'idempotence et la fiabilité de la donnée :

1.  **Ingestion (Couche Bronze) - *Airflow*** :
    * Interrogation incrémentale de l'API Air France-KLM.
    * Stockage des données brutes (JSON dénormalisé) dans Supabase.
    * Gestion automatique des quotas d'API et pagination.
2.  **Transformation (Couche Silver) - *dbt Core*** :
    * Nettoyage, typage et dédoublonnage (Upsert) des vols.
    * Les modèles dbt transforment la donnée brute en un modèle relationnel requêtable, en moins de 5 secondes par exécution.
3.  **Exposition (Couche Serveur) - *FastAPI*** :
    * Point de terminaison principal : `GET /flight-stats`.
    * Construction dynamique des requêtes SQL via SQLAlchemy avec injection sécurisée des paramètres (filtrage par date et compagnie).
4.  **Consommation (Couche Client) - *Streamlit*** :
    * Dashboard interactif mis en cache pour optimiser les appels réseau.
    * Suivi des KPIs en temps réel.

## Quick Start (Déploiement)

*Pour un guide d'installation détaillé étape par étape (Clonage, WSL, Docker), veuillez consulter [docs/INSTALL.md](docs/INSTALL.md).*

**1. Configuration :**
Créer un fichier `.env` à la racine du projet contenant les accès à la base de données (`DB_HOST`, `DB_USER`, etc.) et la clé API AFKLM (`AIRFRANCE_API_KEY`).

**2. Lancement des services :**
docker compose up -d --build

**3. Points d'accès réseau :**

| Service | Technologie | Port Local | Description |
| :--- | :--- | :--- | :--- |
| **Orchestrateur** | Airflow | `8081` | Interface de monitoring des DAGs (Login: airflow/airflow) |
| **Backend API** | FastAPI | `8000` | Documentation interactive Swagger (`/docs`) |
| **Frontend** | Streamlit | `8505` | Dashboard de visualisation utilisateur |
| **Métadonnées** | PostgreSQL | `5440` | Base interne à l'orchestrateur Docker |

## Maintenance et Debugging

Commandes utiles pour l'administration de l'infrastructure locale :

# Vérifier l'état des services
docker compose ps

# Lire les logs de l'API en temps réel
docker logs -f afklm-api

# Lire les logs du Dashboard
docker logs -f afklm-dashboard-final

# Éteindre l'infrastructure sans détruire les volumes de données
docker compose stop