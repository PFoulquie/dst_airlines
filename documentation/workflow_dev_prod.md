# Workflow : promotion de l'environnement de développement vers la production

Ce document présente l'organisation du développement entre les deux dépôts du projet : **dst_airlines** (environnement de développement) et **afklm-delay-pipeline** (environnement de production). Il décrit la répartition des responsabilités, le principe de promotion du code et les étapes à suivre pour livrer un livrable exploitable en production.

---

## 1. Contexte et objectif

Le projet s'appuie sur deux dépôts Git distincts :

- **dst_airlines** : dépôt de développement et d'exploration, dans lequel sont réalisées les expérimentations, les tests et l'implémentation des composants.
- **afklm-delay-pipeline** : dépôt de production, destiné à la livraison et à la présentation du projet. Il contient uniquement le code validé et prêt à être déployé.

Cette séparation permet de conserver un environnement de développement libre (notebooks, prototypes, documentation de travail) tout en disposant d'un livrable propre et exploitable pour la soutenance ou le déploiement.

---

## 2. Répartition des rôles

| Dépôt | Rôle | Contenu |
|-------|------|---------|
| **dst_airlines** (dev) | Exploration, développement et tests | Notebooks, scripts d'ingestion (dlt, AirLabs), documentation, exploration d'API, prototypes ML, etc. |
| **afklm-delay-pipeline** (prod) | Livrable production | Modèles dbt, scripts ML et d'export, configuration, documentation de livraison |

---

## 3. Répartition des fichiers

### Dépôt dst_airlines (développement)

- `1_ingestion/` : scripts d'ingestion (dlt, AirLabs)
- `0_exploration/` : notebooks et exploration des APIs
- `documentation/` : guides, plans, schémas
- `2_silver_processing/` : traitements Silver (AirLabs)
- `1_ingestion/pierre_ML/` : prototype ML (notebook `de.ipynb`)
- `3_ml/` : script ML de production en cours de développement

### Dépôt afklm-delay-pipeline (production)

- `models/` : modèles dbt (raw, int, mart)
- `ml_score.py`, `pg_export.py` : scripts de production
- `dbt_project.yml`, `profiles.yml`, `.env.example` : configuration
- README et documentation de livrable

---

## 4. Principe : promotion du code validé

Le workflow repose sur une **promotion** du code validé de l'environnement de développement vers l'environnement de production. Les deux dépôts coexistent ; le dépôt de production n'est pas recréé par clonage à chaque livraison.

Lorsqu'un composant est validé dans dst_airlines (ingestion dlt, script ML, etc.), il est copié vers afklm-delay-pipeline. Les modèles dbt sont développés directement dans afklm-delay-pipeline, qui constitue le projet dbt.

```
dst_airlines (dev)                    afklm-delay-pipeline (prod)
─────────────────                    ───────────────────────────
Développement et exploration          Livrable production
         │                                     ▲
         │  Code validé                        │
         │  promu par copie                    │
         └─────────────────────────────────────┘
```

---

## 5. Étapes du workflow

### Étape 1 : Développement dans dst_airlines

L'implémentation des composants d'ingestion (dlt) et du script ML se fait dans dst_airlines. Les tests et l'exploration sont réalisés dans cet environnement.

### Étape 2 : Développement des modèles dbt dans afklm-delay-pipeline

Les modèles dbt sont développés directement dans afklm-delay-pipeline, qui est le projet dbt. Les deux dépôts peuvent être ouverts dans le même workspace (VSCode) pour faciliter le travail. Cette approche évite une étape de copie pour les modèles dbt.

### Étape 3 : Validation

Avant toute promotion, la chaîne complète est validée :

1. Exécution du pipeline dlt : les données sont chargées dans Supabase
2. Exécution de dbt : les modèles raw, int et mart sont construits
3. Exécution du script ML : les prédictions sont écrites dans `ml_delays_scored`

### Étape 4 : Promotion vers la production

Les fichiers validés sont copiés de dst_airlines vers afklm-delay-pipeline :

| Fichier source (dev) | Destination (prod) |
|----------------------|-------------------|
| `dst_airlines/1_ingestion/afklm_source.py` | `afklm-delay-pipeline/scripts/` ou `ingestion/` |
| `dst_airlines/1_ingestion/afklm_dlt_pipeline.py` | `afklm-delay-pipeline/scripts/` ou `ingestion/` |
| `dst_airlines/3_ml/ml_score.py` | `afklm-delay-pipeline/` |

### Étape 5 : Versionnement du dépôt de production

Après promotion, les modifications sont versionnées dans le dépôt afklm-delay-pipeline :

```bash
cd afklm-delay-pipeline
git add .
git commit -m "Promotion: dlt + ml_score validés"
git push
```

---

## 6. Structure des dossiers

```
dst_airlines/                          afklm-delay-pipeline/
├── 1_ingestion/                      ├── models/          (dbt)
│   ├── afklm_dlt_pipeline.py         ├── dbt_project.yml
│   ├── afklm_source.py               ├── ml_score.py     (promu depuis dev)
│   └── ingestion_af_klm.py           ├── pg_export.py
├── 0_exploration/                    ├── scripts/         (optionnel : dlt)
├── 2_silver_processing/              │   └── afklm_*.py
├── 3_ml/                             └── README.md
│   └── ml_score.py  ────promotion───► ml_score.py
└── documentation/
```

---

## 7. Script de promotion (optionnel)

Un script peut être utilisé pour automatiser la copie des fichiers validés. Exemple :

```bash
#!/bin/bash
# Promotion dev → prod
# À exécuter depuis la racine du workspace (parent de dst_airlines et Projet_prod)

DEV=dst_airlines
PROD=Projet_prod/afklm-delay-pipeline

mkdir -p $PROD/scripts

cp $DEV/1_ingestion/afklm_source.py $PROD/scripts/
cp $DEV/1_ingestion/afklm_dlt_pipeline.py $PROD/scripts/
cp $DEV/3_ml/ml_score.py $PROD/

echo "Promotion terminée. Vérifier et commit dans afklm-delay-pipeline."
```

Rendre le script exécutable : `chmod +x documentation/promote_to_prod.sh`

---

## 8. Checklist avant livraison ou soutenance

- [ ] Le dépôt afklm-delay-pipeline contient tous les fichiers nécessaires
- [ ] Le README est à jour avec les instructions d'installation et d'exécution
- [ ] Le fichier `.env.example` est documenté (sans secrets)
- [ ] La commande `dbt run` s'exécute correctement
- [ ] Le script `ml_score.py` est exécutable (dépendances listées dans requirements.txt)
- [ ] La pipeline complète a été testée : dlt → dbt → ml_score

---

## 9. Présentation des dépôts

Lors de la soutenance ou de la présentation du projet, les deux dépôts peuvent être présentés ainsi :

- **dst_airlines** : dépôt de développement et d'exploration, contenant les notebooks, les prototypes et la documentation de travail.
- **afklm-delay-pipeline** : livrable de production, prêt à être déployé, contenant uniquement le code validé et exploitable.
- Les deux dépôts sont ouverts dans le même workspace pour faciliter le développement et la promotion du code.
