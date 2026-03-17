# Guide dlt (data load tool) — Pipeline AF/KLM

Ce document explique comment **dlt** fonctionne, comment la pipeline AF/KLM est implémentée, et comment la lancer.

---

## Table des matières

1. [Qu'est-ce que dlt ?](#1-quest-ce-que-dlt)
2. [Les 3 phases d'un run](#2-les-3-phases-dun-run)
3. [Concepts clés : Pipeline, Source, Resource](#3-concepts-clés--pipeline-source-resource)
4. [Write disposition (mode d'écriture)](#4-write-disposition-mode-décriture)
5. [Secrets et configuration](#5-secrets-et-configuration)
6. [Comment lancer la pipeline](#6-comment-lancer-la-pipeline)
7. [Fichiers du pipeline](#7-fichiers-du-pipeline)
8. [Walkthrough du code](#8-walkthrough-du-code)
9. [Intégration avec dbt](#9-intégration-avec-dbt)
10. [Points d'attention spécifiques à l'API AF/KLM](#10-points-dattention-spécifiques-à-lapi-afklm)

---

## 1. Qu'est-ce que dlt ?

**dlt** (data load tool) est une librairie Python open-source qui automatise l'**Extract & Load (EL)** : elle prend des données depuis une source (API, base de données, fichiers, etc.) et les charge dans une destination (PostgreSQL/Supabase, BigQuery, DuckDB, etc.) avec inférence de schéma, gestion des erreurs et chargement incrémental.

### Pourquoi dlt plutôt que requests et SQL manuel ?

| Aspect | Avec requests + SQL manuel | Avec dlt |
|--------|----------------------------|----------|
| **Schéma** | Définir manuellement les colonnes, gérer les types, adapter aux changements d'API | Inférence automatique du schéma, évolution détectée et gérée |
| **Chargement incrémental** | Implémenter watermarks, déduplication, logique de merge soi-même | `write_disposition="merge"` et `primary_key` gèrent l'upsert |
| **Erreurs** | Gérer retry, backoff, timeouts à la main | Mécanismes intégrés, state persisté pour reprendre après une erreur |
| **Secrets** | Variables d'environnement ou `.env` gérés manuellement | `.dlt/secrets.toml` centralisé, `dlt.secrets.value` |
| **Responsabilités** | Extraction, transformation et chargement mélangés | dlt : EL + normalisation structurelle ; dbt : transformations métier |

**Exemples concrets pour la pipeline AF/KLM :**

- **Schéma** : L'API AF/KLM renvoie `operationalFlights[]` avec des champs imbriqués (`airline.code`, `flightLegs[].departureInformation.times`). dlt infère le schéma automatiquement ; si l'API ajoute un champ, dlt l'intègre sans migration manuelle.

- **Chargement incrémental** : Un même vol (`id = "20260111+AF+0605"`) peut être renvoyé plusieurs fois avec un statut mis à jour. `write_disposition="merge"` avec `primary_key="id"` gère l'upsert automatiquement.

- **Secrets** : Les clés API et credentials Supabase sont dans `.dlt/secrets.toml` (ignoré par Git). Plus de `os.getenv()` dispersé dans le code.

### Rôle dans l'architecture

```
API AF/KLM  ──►  dlt (EL)  ──►  Supabase (public)  ──►  dbt (Transform)  ──►  mart
```

dlt assure **uniquement** l'extraction et le chargement. La transformation métier (retards, agrégations, schéma en étoile) reste dans dbt.

---

## 2. Les 3 phases d'un run

Quand tu appelles `pipeline.run(source)`, dlt exécute **3 phases** dans l'ordre :

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  EXTRACT    │ ──► │  NORMALIZE  │ ──► │    LOAD     │
│             │     │             │     │             │
│ Données     │     │ Inférence   │     │ Migrations  │
│ vers disque │     │ schéma,     │     │ schéma +    │
│ (load pkg)  │     │ dénestage   │     │ INSERT      │
└─────────────┘     └─────────────┘     └─────────────┘
```

| Phase | Rôle |
|-------|------|
| **Extract** | Lit les données depuis la source et les écrit sur disque dans un *load package* (dans `~/.dlt/pipelines/afklm/`) |
| **Normalize** | Infère le schéma, gère les structures imbriquées (listes → tables enfants), détecte les types |
| **Load** | Applique les migrations de schéma sur la destination et charge les données (INSERT/MERGE) |

> **Note** : Le champ `route` (tableau de codes aéroports) est automatiquement normalisé par dlt en une table enfant `operational_flights__route`, sans intervention manuelle.

---

## 3. Concepts clés : Pipeline, Source, Resource

### Pipeline

Objet central qui orchestre le flux :

```python
pipeline = dlt.pipeline(
    pipeline_name="afklm",    # Nom utilisé pour le state local (~/.dlt/pipelines/afklm/)
    destination="postgres",   # Connecteur de destination (postgres = Supabase)
    dataset_name="public",    # Schéma PostgreSQL cible
)
```

### Source (`@dlt.source`)

Regroupe plusieurs **resources**. C'est une fonction décorée qui yield des resources :

```python
@dlt.source(name="afklm")
def afklm_source(api_key=dlt.secrets.value, ...):
    yield operational_flights_resource(all_flights_rows)
    yield operational_flight_legs_resource(all_legs_rows)
    yield operational_flight_delays_resource(all_delays_rows)
```

### Resource (`@dlt.resource`)

Un flux de données = une table cible. Reçoit une liste de dicts et les yield vers dlt :

```python
@dlt.resource(name="operational_flights", write_disposition="merge", primary_key="id")
def operational_flights_resource(flights_rows):
    yield from flights_rows   # chaque dict = une ligne dans la table
```

---

## 4. Write disposition (mode d'écriture)

| Valeur | Comportement |
|--------|--------------|
| `append` | Ajoute les lignes (défaut) |
| `replace` | Remplace toute la table à chaque run |
| `merge` | **Upsert** : met à jour si la clé existe, insère sinon. Nécessite `primary_key` |

Pour la pipeline AF/KLM, **`merge`** avec `primary_key="id"` est utilisé sur les 3 tables : un même vol peut être renvoyé par l'API avec un statut mis à jour (ex. `flightStatusPublic` passe de `"OnTime"` à `"Delayed"`), le merge garantit qu'il ne sera pas dupliqué.

---

## 5. Secrets et configuration

### `.dlt/secrets.toml` — données sensibles (ne pas versionner)

```toml
[sources.afklm]
api_key = "votre_clé_api_afklm"       # Clé API Air France/KLM

[destination.postgres.credentials]
host     = "db.xxx.supabase.co"        # Host Supabase (Project Settings > Database)
port     = 5432                        # Port PostgreSQL standard
database = "postgres"                  # Nom de la base (toujours "postgres" sur Supabase)
username = "postgres"                  # Utilisateur Supabase
password = "votre_mot_de_passe"        # Mot de passe du projet Supabase
sslmode  = "require"                   # SSL obligatoire sur Supabase
```

- `dlt.secrets.value` dans le code indique à dlt de lire la valeur depuis ce fichier.
- Ce fichier est dans `.gitignore`. Ne jamais le committer.
- Voir `secrets.toml.example` pour le template.

### `.dlt/config.toml` — configuration non sensible (versionnable)

```toml
[runtime]
log_level = "INFO"       # Niveau de log : DEBUG, INFO, WARNING, ERROR

[sources.afklm]
# Fenêtre temporelle fixe (optionnel — si absent, comportement incrémental)
start_date = "2026-01-16T00:00:00Z"
end_date   = "2026-01-16T02:00:00Z"

[destination.postgres]
dataset_name = "public"  # Schéma PostgreSQL cible
```

- `dlt.config.value` dans le code lit depuis ce fichier.
- En mode incrémental (sans `start_date`/`end_date` ou avec `incremental=True`), dlt reprend depuis la dernière `end_date` mémorisée dans son state.

---

## 6. Comment lancer la pipeline

### Prérequis

- Python 3.11+
- Environnement virtuel activé
- `.dlt/secrets.toml` configuré
- Tables Supabase créées (voir `documentation/supabase_ddl_option_b.sql`)

### Étapes

```bash
# 1. Se placer à la racine du projet
cd ~/Documents/APPRENTISSAGE/dst_airlines

# 2. Activer l'environnement virtuel
source venv/bin/activate

# 3. (Optionnel) Ajuster la fenêtre temporelle dans .dlt/config.toml
#    start_date / end_date → fenêtre fixe
#    Supprimer ces lignes  → mode incrémental (reprend depuis le dernier run)

# 4. Lancer la pipeline
python 1_ingestion/afklm_dlt_pipeline.py

# 5. (Optionnel) Voir les logs détaillés
python -u 1_ingestion/afklm_dlt_pipeline.py 2>&1 | tee pipeline_$(date +%Y%m%d_%H%M).log
```

### Ce que produit un run réussi

```
Fetch terminé : 876 vols, 1114 legs, 20 delays
Pipeline afklm load step completed in 2.52 seconds
1 load package(s) were loaded to destination postgres and into dataset public
Load package 1773688980.240997 is LOADED and contains no failed jobs
```

### Mode incrémental vs fenêtre fixe

| Mode | Config | Comportement |
|------|--------|--------------|
| **Fenêtre fixe** | `start_date` et `end_date` dans `config.toml` | Toujours la même plage, peu importe les runs précédents |
| **Incrémental** | Supprimer `start_date`/`end_date` (ou `incremental=True`) | Reprend depuis la `end_date` du dernier run réussi (mémorisée par dlt dans son state) |

> Pour **réinitialiser le state** (repartir de zéro) : supprimer `~/.dlt/pipelines/afklm/`

### Séquence batch complète (dlt → dbt → ML)

```bash
# Ingestion
python 1_ingestion/afklm_dlt_pipeline.py

# Transformation dbt (depuis le repo afklm-delay-pipeline)
cd ../Projet_prod/afklm-delay-pipeline
dbt run

# Scoring ML
cd ~/Documents/APPRENTISSAGE/dst_airlines
python 3_ml/ml_score.py
```

Ou via le script orchestrateur :

```bash
bash run_pipeline.sh
```

---

## 7. Fichiers du pipeline

```
dst_airlines/
├── .dlt/
│   ├── config.toml           # Fenêtre temporelle, log level, schéma cible (versionnable)
│   └── secrets.toml          # Clé API + credentials Supabase (NE PAS VERSIONNER)
├── 1_ingestion/
│   ├── afklm_source.py       # Source dlt : appels API, pagination, normalisation → 3 tables
│   └── afklm_dlt_pipeline.py # Point d'entrée : crée le pipeline dlt et lance le run
└── documentation/
    ├── supabase_ddl_option_b.sql        # DDL des tables Supabase (à créer avant le 1er run)
    └── supabase_migration_dlt_strings.sql # Migration pour supprimer les FK (chargement parallèle)
```

### `afklm_source.py`

Contient toute la logique d'extraction et de normalisation :

| Fonction / Objet | Rôle |
|---|---|
| `PAGE_SIZE = 50` | Nombre de vols par page API (limite API AF/KLM) |
| `SLEEP_BETWEEN_REQUESTS = 5` | Pause entre les pages pour respecter le rate limit |
| `RETRY_BACKOFF_500` | Délais croissants (30s, 60s, 90s, 120s, 180s) entre retries sur erreurs 5xx |
| `_fetch_page()` | Appelle l'API pour une fenêtre + page donnée. Retry sur 5xx, lève immédiatement sur 4xx |
| `_iter_flights()` | Itère sur toutes les pages d'une fenêtre temporelle, yield `(flight, fetched_at)` |
| `_get_dates()` | Détermine la fenêtre à extraire (config fixe ou state incrémental) |
| `_build_flights_table()` | Transforme un vol brut en dict pour `operational_flights` |
| `_build_legs_table()` | Transforme un vol brut en liste de dicts pour `operational_flight_legs` |
| `_build_delays_table()` | Transforme un vol brut en liste de dicts pour `operational_flight_delays` |
| `operational_flights_resource` | Resource dlt → table `operational_flights` (merge sur `id`) |
| `operational_flight_legs_resource` | Resource dlt → table `operational_flight_legs` (merge sur `id`) |
| `operational_flight_delays_resource` | Resource dlt → table `operational_flight_delays` (merge sur `id`) |
| `afklm_source()` | Source dlt : orchestre le fetch unique, dispatch vers les 3 resources |

### `afklm_dlt_pipeline.py`

Point d'entrée minimal : crée le pipeline dlt et lance le run sur la source.

---

## 8. Walkthrough du code

### `afklm_dlt_pipeline.py`

```python
import sys
from pathlib import Path

# Ajoute le dossier 1_ingestion/ au PYTHONPATH pour que l'import
# de afklm_source fonctionne quel que soit le répertoire de lancement.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import dlt
from afklm_source import afklm_source

if __name__ == "__main__":
    # Crée le pipeline dlt :
    # - pipeline_name : identifiant local du pipeline (state dans ~/.dlt/pipelines/afklm/)
    # - destination   : connecteur postgres (credentials lus depuis .dlt/secrets.toml)
    # - dataset_name  : schéma PostgreSQL cible (public = schéma par défaut Supabase)
    pipeline = dlt.pipeline(
        pipeline_name="afklm",
        destination="postgres",
        dataset_name="public",
    )

    # Lance les 3 phases : Extract → Normalize → Load
    # afklm_source() lit api_key depuis secrets.toml et start/end_date depuis config.toml
    load_info = pipeline.run(afklm_source())

    # Affiche le résumé du run (nb de lignes chargées, erreurs éventuelles)
    print(load_info)
```

### `afklm_source.py` — constantes

```python
PAGE_SIZE = 50              # Vols par page. L'API AF/KLM renvoie 500 sur 5xx si > ~100.
SLEEP_BETWEEN_REQUESTS = 5  # Secondes entre deux pages (anti rate-limit).
MAX_RETRIES = 5             # Nb maximum de tentatives sur erreur 5xx.
RETRY_BACKOFF_500 = [30, 60, 90, 120, 180]
# Backoff progressif : 30s après la 1ère erreur, 60s après la 2ème, etc.
# Les 5xx AF/KLM sont souvent des throttles déguisés, pas de vraies erreurs serveur.
```

### `_fetch_page()` — appel HTTP + retry

```python
def _fetch_page(api_key, start_range, end_range, page_number=0):
    url = f"{BASE_URL}/flightstatus"
    params = {
        "startRange": start_range,   # ex. "2026-01-16T00:00:00.000Z"
        "endRange":   end_range,     # ex. "2026-01-16T01:00:00.000Z"
        "pageSize":   PAGE_SIZE,     # Nb de vols par page
        "pageNumber": page_number,   # Index de la page (commence à 0)
    }
    headers = {
        "API-Key": api_key,              # Authentification AF/KLM
        "Accept": "application/hal+json" # Format de réponse attendu
    }
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=30)
            resp.raise_for_status()   # Lève une exception sur tout code HTTP >= 400
            return resp.json()
        except requests.exceptions.RequestException as e:
            resp_obj = getattr(e, "response", None)
            if resp_obj is not None and resp_obj.status_code in (500, 502, 503, 504):
                # Erreur serveur transitoire → attendre et retenter
                time.sleep(RETRY_BACKOFF_500[min(attempt, len(RETRY_BACKOFF_500) - 1)])
                continue
            # 4xx (403 = quota dépassé, 400 = mauvais paramètre) ou erreur réseau
            # → lever immédiatement pour éviter une boucle infinie
            raise
```

### `_iter_flights()` — pagination par jour

```python
def _iter_flights(api_key, start_date, end_date):
    current = start_date
    # Capture l'heure de début du run pour l'horodater dans fetched_at.
    # Une valeur unique par run garantit la cohérence pour dbt (toutes les lignes
    # d'un même run ont le même fetched_at).
    fetched_at = datetime.now(timezone.utc).isoformat()

    while current < end_date:
        # Découpe la fenêtre par tranches d'1 jour max car l'API AF/KLM
        # renvoie des résultats incohérents ou tronqués sur de larges fenêtres.
        window_end = min(current + timedelta(days=1), end_date)
        start_range = current.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        end_range   = window_end.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        # Page 0 : récupère aussi totalPages pour savoir combien de pages itérer
        data = _fetch_page(api_key, start_range, end_range, 0)
        total_pages = data.get("page", {}).get("totalPages", 0)
        flights = data.get("operationalFlights", [])

        for flight in flights:
            yield flight, fetched_at  # yield = lazy, économise la mémoire

        # Pages suivantes (1 à N-1)
        for page_num in range(1, total_pages):
            time.sleep(SLEEP_BETWEEN_REQUESTS)  # Pause anti rate-limit entre pages
            try:
                page_data = _fetch_page(api_key, start_range, end_range, page_num)
            except requests.exceptions.RequestException as e:
                # Si une page échoue après tous les retries, on la saute (skip)
                # plutôt que d'arrêter tout le run. Données partielles préférées
                # à zéro données.
                logger.warning("Page %d échouée après retries, skip : %s", page_num, e)
                continue
            for flight in page_data.get("operationalFlights", []):
                yield flight, fetched_at

        current = window_end   # Avancer d'un jour
```

### `_get_dates()` — fenêtre incrémentale ou fixe

```python
def _get_dates(start_date, end_date, incremental=True):
    state = dlt.current.source_state()  # Dict persisté par dlt entre les runs
    last_end = state.get("last_window_end") if incremental else None

    if last_end:
        # Mode incrémental : reprend depuis la fin du dernier run réussi
        start = datetime.fromisoformat(last_end.replace("Z", "+00:00"))
    elif start_date:
        # Fenêtre fixe : utilise la valeur de config.toml
        start = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
    else:
        # Fallback : hier (utile pour un premier run sans config)
        start = datetime.now(timezone.utc) - timedelta(days=1)

    end = datetime.fromisoformat(end_date.replace("Z", "+00:00")) if end_date \
          else datetime.now(timezone.utc)
    return start, end
```

### `_build_flights_table()` — mapping vol → dict

```python
def _build_flights_table(flight, fetched_at):
    airline = flight.get("airline") or {}  # Guard : si "airline" est null dans l'API
    return {
        "id": flight.get("id"),                    # ex. "20260116+AF+0605"
        "flight_number": flight.get("flightNumber"),
        "flight_schedule_date": flight.get("flightScheduleDate"),  # String brute ("2026-01-16")
        # Le cast en DATE sera fait en dbt 1_raw (::date).
        "airline_code": airline.get("code"),       # ex. "AF"
        "airline_name": airline.get("name"),       # ex. "Air France"
        "haul": flight.get("haul"),                # ex. "LONG" ou "SHORT"
        "route": flight.get("route"),              # Liste ["CDG", "JFK"] → table enfant auto dlt
        "flight_status_public": flight.get("flightStatusPublic"),
        "fetched_at": fetched_at,                  # Horodatage du run
    }
```

### `_build_legs_table()` — mapping legs → dicts

```python
def _build_legs_table(flight):
    flight_id = flight.get("id")
    rows = []
    for i, leg in enumerate(flight.get("flightLegs") or []):
        # Identifiant déterministe : uuid5 basé sur flight_id + position du leg.
        # Déterministe = si le même vol est rechargé, le même UUID est généré.
        # Cela garantit que le merge dlt (primary_key="id") fonctionne correctement.
        leg_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{flight_id}_{i}"))

        dep_info   = leg.get("departureInformation") or {}
        arr_info   = leg.get("arrivalInformation") or {}
        dep_airport = dep_info.get("airport") or {}
        arr_airport = arr_info.get("airport") or {}
        dep_times  = dep_info.get("times") or {}
        arr_times  = arr_info.get("times") or {}
        irreg      = leg.get("irregularity") or {}

        rows.append({
            "id": leg_id,
            "flight_id": flight_id,
            "leg_order": i,                    # Position du leg dans le vol (0 = premier)
            "departure_airport_code": dep_airport.get("code"),
            "arrival_airport_code":   arr_airport.get("code"),
            "published_status": leg.get("publishedStatus"),
            "scheduled_departure": dep_times.get("scheduled"),  # ISO 8601 string
            "actual_departure":    dep_times.get("actual"),
            "scheduled_arrival":   arr_times.get("scheduled"),
            "actual_arrival":      arr_times.get("actual"),
            "scheduled_flight_duration": leg.get("scheduledFlightDuration"),
            "cancelled": irreg.get("cancelled") == "Y",  # Converti en booléen
            "aircraft_type_code": (leg.get("aircraft") or {}).get("typeCode"),
        })
    return rows
```

### `_build_delays_table()` — mapping retards → dicts

```python
def _build_delays_table(flight):
    flight_id = flight.get("id")
    rows = []
    for i, leg in enumerate(flight.get("flightLegs") or []):
        # Même UUID déterministe que dans _build_legs_table pour assurer la cohérence
        # entre flight_leg_id ici et id dans operational_flight_legs.
        leg_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{flight_id}_{i}"))
        irreg = leg.get("irregularity") or {}

        # L'API renvoie les retards dans deux formats possibles :
        delay_infos = irreg.get("delayInformation") or []
        if not delay_infos:
            # Format alternatif : deux listes parallèles (codes + durées)
            codes     = irreg.get("delayCode") or []
            durations = irreg.get("delayDuration") or []
            delay_infos = [{"delayCode": c, "delayDuration": d}
                           for c, d in zip(codes, durations)]

        for j, d in enumerate(delay_infos):
            rows.append({
                # UUID déterministe = flight_id + position leg + position delay
                "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{flight_id}_{i}_{j}")),
                "flight_leg_id": leg_id,
                "delay_code":     d.get("delayCode"),     # Code IATA du retard (ex. "93")
                "delay_duration": d.get("delayDuration"), # Durée en minutes (string)
            })
    return rows
```

### `afklm_source()` — orchestration

```python
@dlt.source(name="afklm")
def afklm_source(
    api_key: str = dlt.secrets.value,      # Lu depuis .dlt/secrets.toml [sources.afklm]
    start_date: str | None = dlt.config.value,  # Lu depuis .dlt/config.toml [sources.afklm]
    end_date:   str | None = dlt.config.value,
    incremental: bool = True,
):
    start, end = _get_dates(start_date, end_date, incremental)

    all_flights_rows = []
    all_legs_rows    = []
    all_delays_rows  = []

    # UN seul passage sur l'API — les 3 tables sont alimentées depuis le même flux.
    # Cela évite de faire 3× les appels API (un par resource).
    for flight, fetched_at in _iter_flights(api_key, start, end):
        all_flights_rows.append(_build_flights_table(flight, fetched_at))
        all_legs_rows.extend(_build_legs_table(flight))
        all_delays_rows.extend(_build_delays_table(flight))

    # Yield les 3 resources → dlt les charge en parallèle dans Supabase.
    # Les FK ont été supprimées des tables Supabase pour permettre ce chargement
    # parallèle (voir supabase_migration_dlt_strings.sql).
    yield operational_flights_resource(all_flights_rows)
    yield operational_flight_legs_resource(all_legs_rows)
    yield operational_flight_delays_resource(all_delays_rows)

    # Mémorise la fin de la fenêtre dans le state dlt pour le prochain run incrémental.
    try:
        dlt.current.source_state()["last_window_end"] = end.isoformat()
    except Exception:
        pass  # Silencieux si le state n'est pas disponible (tests, dry-run)
```

---

## 9. Intégration avec dbt

dlt charge les données brutes dans `public.*`. dbt lit ces tables comme sources et applique :

1. **1_raw** : casting des types (`::date`, `::uuid`), renommage des colonnes
2. **int** : jointures (legs ↔ flights, delays ↔ legs), calculs de retards
3. **mart** : faits et dimensions (schéma en étoile pour l'analyse et le ML)

```
public.operational_flights        ─┐
public.operational_flight_legs    ─┤─► dbt raw ─► dbt int ─► mart.fct_flight_legs
public.operational_flight_delays  ─┘
public.operational_flights__route ─► dbt raw ─► (si nécessaire)
```

Séquence batch :

```bash
python 1_ingestion/afklm_dlt_pipeline.py  # Ingestion
dbt run                                    # Transformation
python 3_ml/ml_score.py                   # Scoring ML
```

---

## 10. Points d'attention spécifiques à l'API AF/KLM

| Point | Détail |
|-------|--------|
| **Pagination** | L'API ne supporte pas offset/limit classique. Il faut une fenêtre temporelle + `pageNumber` (de 0 à `totalPages - 1`) |
| **Rate limits** | Les erreurs 500 sont souvent des throttles déguisés. D'où le `SLEEP_BETWEEN_REQUESTS` et le `RETRY_BACKOFF_500` progressif |
| **Fenêtre max** | Dépasser ~24h par fenêtre peut produire des résultats tronqués ou incohérents |
| **Format de réponse** | HAL+JSON avec `operationalFlights[]` et `page.totalPages` |
| **Authentification** | Header `API-Key: <votre_clé>` (pas de Bearer token) |
| **Endpoint** | `GET https://api.airfranceklm.com/opendata/flightstatus?startRange=...&endRange=...&pageNumber=...&pageSize=...` |
| **FK supprimées** | Les tables Supabase n'ont pas de contraintes FK entre elles. dlt charge en parallèle, l'ordre d'insertion n'est pas garanti. L'intégrité est assurée par la source API (un leg appartient toujours à un vol existant) |

---

## Références

- [How dlt works](https://dlthub.com/docs/reference/explainers/how-dlt-works)
- [Source](https://dlthub.com/docs/general-usage/source)
- [Resource](https://dlthub.com/docs/general-usage/resource)
- [REST API to Supabase](https://dlthub.com/docs/pipelines/rest_api/load-data-with-python-from-rest_api-to-supabase)
