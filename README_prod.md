# Pipeline AF/KLM — Dev & Prod

Documentation pour l'installation et l'exécution de la pipeline de prédiction des retards AF/KLM.  
**Même pipeline en dev et en prod** : seule la configuration (secrets, base) change.

---

## Vue d'ensemble

```
API AF/KLM → dlt (EL) → Supabase → dbt (Transform) → mart.fct_flight_legs → ml_score.py → ml_delays_scored
```

| Étape | Outil | Rôle |
|-------|-------|------|
| 1 | dlt | Extraction API + chargement dans 3 tables (Option B) |
| 2 | dbt | Transformations raw → int → mart |
| 3 | ml_score.py | Modèle ML, prédictions dans `ml_delays_scored` |

---

## Prérequis

- Python 3.10+
- Accès API AF/KLM
- Base Supabase (PostgreSQL)

---

## Installation des dépendances

### 1. Environnement virtuel (recommandé)

```bash
cd dst_airlines
python3 -m venv venv
source venv/bin/activate   # macOS/Linux
# ou: venv\Scripts\activate  # Windows
```

### 2. Installation des packages

```bash
pip install -r requirements.txt
```

**Packages principaux :**

| Package | Usage |
|---------|-------|
| `dlt[postgres]` | Ingestion API → Supabase |
| `scikit-learn` | Modèle ML |
| `xgboost` | Modèle ML (optionnel) |
| `joblib` | Sauvegarde du modèle |
| `imbalanced-learn` | SMOTE (déséquilibre des classes) |
| `dbt-postgres` | Transformations SQL |
| `requests`, `pandas`, `sqlalchemy`, `psycopg2-binary` | Connexions et données |

### 3. Vérification

```bash
python -c "import dlt; import sklearn; import xgboost; import joblib; import imblearn; print('OK')"
```

---

## Configuration

### 1. Secrets dlt (`.dlt/secrets.toml`)

Copier le modèle et renseigner les valeurs :

```bash
cp .dlt/secrets.toml.example .dlt/secrets.toml
```

Exemple :

```toml
[sources.afklm]
api_key = "votre_cle_api_afklm"

[destination.postgres.credentials]
host = "db.xxx.supabase.co"
port = 5432
database = "postgres"
username = "postgres"
password = "votre_mot_de_passe"
```

Variables d'environnement possibles : `api_key = "env:AFKLM_API_KEY"`, `password = "env:SUPABASE_PASSWORD"`.

### 2. DDL Supabase

Exécuter dans le SQL Editor Supabase le contenu de :

```
documentation/supabase_ddl_option_b.sql
```

Création des tables : `operational_flights`, `operational_flight_legs`, `operational_flight_delays`, schémas `raw`, `int`, `mart`.

### 3. dbt (profiles.yml)

Créer `~/.dbt/profiles.yml` (ou copier `afklm-delay-pipeline/profiles.yml.example`) :

```yaml
afklm_delay_pipeline:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('AFKLM_DB_HOST') }}"
      port: "{{ env_var('AFKLM_DB_PORT', '5432') }}"
      user: "{{ env_var('AFKLM_DB_USER') }}"
      password: "{{ env_var('AFKLM_DB_PASSWORD') }}"
      dbname: "{{ env_var('AFKLM_DB_NAME', 'postgres') }}"
      schema: public
      threads: 4
```

### 4. Variables d'environnement (dbt + ML)

```bash
export AFKLM_DB_HOST=db.xxx.supabase.co
export AFKLM_DB_PORT=5432
export AFKLM_DB_USER=postgres
export AFKLM_DB_PASSWORD=xxx
export AFKLM_DB_NAME=postgres
```

---

## Exécution

### Pipeline complète

```bash
./run_pipeline.sh
```

Ou manuellement :

```bash
# 1. Ingestion
python 1_ingestion/afklm_dlt_pipeline.py

# 2. Transformations dbt
cd ../Projet_prod/afklm-delay-pipeline
dbt run

# 3. ML
cd ../dst_airlines
python 3_ml/ml_score.py
```

### Ingestion seule (test)

```bash
python 1_ingestion/afklm_dlt_pipeline.py
```

---

## Différence Dev / Prod

| Aspect | Dev | Prod |
|--------|-----|------|
| Base Supabase | Projet / instance de test | Supabase de production |
| Secrets | `.dlt/secrets.toml` local | Variables d'environnement ou secrets |
| Orchestration | Lancement manuel ou `run_pipeline.sh` | Cron, Airflow, etc. |

---

## Structure des dossiers

```
dst_airlines/
├── .dlt/
│   ├── config.toml
│   └── secrets.toml
├── 1_ingestion/
│   ├── afklm_source.py
│   ├── afklm_dlt_pipeline.py
│   └── ingestion_af_klm.py
├── 3_ml/
│   └── ml_score.py
├── documentation/
│   └── supabase_ddl_option_b.sql
├── requirements.txt
├── run_pipeline.sh
└── README_prod.md
```
