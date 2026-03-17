# Protocole : Pipeline AF/KLM (dlt + dbt + ML)

Récapitulatif pas à pas de la mise en place et de l'exécution du pipeline de prédiction des retards AF/KLM.

---

## Vue d'ensemble

```
API AF/KLM → dlt (EL) → Supabase (3 tables) → dbt (Transform) → mart.fct_flight_legs → ml_score.py → ml_delays_scored
```

---

## Étape 1 : Environnement Python

| Action | Commande / Fichier |
|--------|--------------------|
| Installer les dépendances | `pip install -r requirements.txt` |
| Vérifier | `dlt`, `dbt`, `scikit-learn`, `xgboost`, `imbalanced-learn` présents |

---

## Étape 2 : API AF/KLM — Création du compte développeur

1. Aller sur **https://developer.airfranceklm.com**
2. Créer un compte (Register) ou se connecter
3. Accéder au **Developer Portal** → **My Apps** (ou **API Keys**)
4. Créer une application pour obtenir une **API Key**
5. Noter la clé : elle sera utilisée dans `secrets.toml` (clé `api_key`)
6. Endpoint utilisé : `GET https://api.airfranceklm.com/opendata/flightstatus` (startRange, endRange, pageNumber, pageSize)

---

## Étape 3 : Configuration dlt (`.dlt/`)

### 3.1 `config.toml` (versionnable)

| Paramètre | Valeur | Rôle |
|-----------|--------|------|
| `log_level` | `WARNING` | Niveau de log du pipeline |
| `[sources.afklm]` | (vide) | `api_key` vient de secrets.toml |
| `dataset_name` | `public` | Schéma Supabase où dlt crée les tables |

**Optionnel** : ajouter `start_date` et `end_date` dans `[sources.afklm]` pour fixer la fenêtre de chargement.

### 3.2 `secrets.toml` (ne pas versionner)

Copier `secrets.toml.example` → `secrets.toml` et remplir avec la clé API AF/KLM et les identifiants Supabase :

| Paramètre | Où trouver |
|-----------|------------|
| `api_key` | developer.airfranceklm.com → My Apps → API Key |
| `host` | Supabase → Settings → Database → Host |
| `port` | 5432 |
| `database` | postgres |
| `username` | postgres |
| `password` | Supabase → Settings → Database |
| `sslmode` | `require` (obligatoire pour Supabase) |

Vérifier que `.dlt/secrets.toml` est dans `.gitignore`.

---

## Étape 4 : Supabase — Création des schémas et tables

1. Ouvrir **Supabase** → **SQL Editor**
2. Coller et exécuter le contenu de `documentation/supabase_ddl_option_b.sql`
3. Vérifier la création :
   - **Schémas** : `raw`, `int`, `mart` (pour dbt)
   - **Tables** dans `public` : `operational_flights`, `operational_flight_legs`, `operational_flight_delays`
   - **Index** : `idx_operational_flight_legs_flight_id`, `idx_operational_flight_delays_flight_leg_id`

---

## Étape 5 : Configuration dbt

1. Copier `afklm-delay-pipeline/profiles.yml.example` → `~/.dbt/profiles.yml`
2. Définir les variables d'environnement (ou remplacer par les valeurs réelles) :

```bash
export AFKLM_DB_HOST="db.xxx.supabase.co"
export AFKLM_DB_PORT="5432"
export AFKLM_DB_USER="postgres"
export AFKLM_DB_PASSWORD="votre_mot_de_passe"
export AFKLM_DB_NAME="postgres"
```

---

## Étape 6 : Exécution

### Test isolé (dlt uniquement)

```bash
cd dst_airlines
python 1_ingestion/afklm_dlt_pipeline.py
```

### Test isolé (dbt uniquement, après dlt)

```bash
cd Projet_prod/afklm-delay-pipeline
dbt run
```

### Pipeline complet

```bash
cd dst_airlines
./run_pipeline.sh
```

Séquence : **dlt** → **dbt** → **ml_score.py**

---

## Étape 7 : Vérifications

| Étape | À contrôler |
|-------|--------------|
| dlt | Tables `public.operational_*` remplies dans Supabase |
| dbt | Vues/tables dans `raw`, `int`, `mart` |
| ML | Table `public.ml_delays_scored` créée avec prédictions |

---

## Dépannage

| Problème | Solution |
|----------|----------|
| Erreur connexion Supabase | Vérifier `sslmode = "require"` dans secrets.toml |
| Clé API invalide | Vérifier sur developer.airfranceklm.com |
| dbt : source introuvable | Vérifier `profiles.yml` et variables `AFKLM_DB_*` |
| ml_score : table vide | Exécuter dbt avant (mart.fct_flight_legs doit exister) |

---

## Fichiers clés

| Fichier | Rôle |
|---------|------|
| `.dlt/config.toml` | Config pipeline dlt (non sensible) |
| `.dlt/secrets.toml` | Secrets API + Supabase (ne pas versionner) |
| `1_ingestion/afklm_source.py` | Source dlt (3 ressources Option B) |
| `1_ingestion/afklm_dlt_pipeline.py` | Point d'entrée ingestion |
| `documentation/supabase_ddl_option_b.sql` | DDL des 3 tables + schémas dbt |
| `run_pipeline.sh` | Orchestration complète |
| `3_ml/ml_score.py` | Modèle ML (Random Forest, prédictions) |
