#!/usr/bin/env bash
# Pipeline AFKLM complet : dlt → dbt → ML
# Exécuter depuis la racine du workspace (dst_airlines ou parent)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DST_ROOT="$(cd "$SCRIPT_DIR" && pwd)"
AFKLM_DBT="${DST_ROOT}/../Projet_prod/afklm-delay-pipeline"

echo "=== Étape 1 : Ingestion dlt ==="
cd "$DST_ROOT"
python 1_ingestion/afklm_dlt_pipeline.py

echo "=== Étape 2 : Transformation dbt ==="
cd "$AFKLM_DBT"
dbt run

echo "=== Étape 3 : ML (prédiction retards) ==="
cd "$DST_ROOT"
python 3_ml/ml_score.py

echo "=== Pipeline terminé ==="
