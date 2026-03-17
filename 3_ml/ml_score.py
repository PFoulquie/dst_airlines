"""
Script ML pour la prédiction des retards AF/KLM.
Lit mart.fct_flight_legs, entraîne un modèle de classification (is_delayed >= 15 min),
écrit les prédictions dans ml_delays_scored.
"""

import os
import re
from pathlib import Path

import joblib
import pandas as pd
from imblearn.over_sampling import SMOTE
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    auc,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
    roc_curve,
)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sqlalchemy import create_engine, text

# Config DB depuis variables d'environnement
DB_HOST = os.getenv("AFKLM_DB_HOST", "localhost")
DB_PORT = os.getenv("AFKLM_DB_PORT", "5432")
DB_USER = os.getenv("AFKLM_DB_USER", "postgres")
DB_PASSWORD = os.getenv("AFKLM_DB_PASSWORD", "")
DB_NAME = os.getenv("AFKLM_DB_NAME", "postgres")
DB_SSLMODE = os.getenv("AFKLM_DB_SSLMODE", "prefer")

DB_URI = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode={DB_SSLMODE}"

NUMERIC_FEATURES = [
    "scheduled_flight_duration_min",
    "departure_weekday",
    "departure_month",
    "departure_hour",
    "departure_monthday",
    "dep_airport_nb_departing",
    "dep_airport_nb_arriving",
    "arr_airport_nb_departing",
    "arr_airport_nb_arriving",
]
CATEGORICAL_FEATURES = ["airline_key", "aircraft_type_code"]
TARGET = "is_delayed"


def parse_iso8601_duration(s: str) -> int:
    """Parse PT2H25M -> 145 minutes."""
    if pd.isna(s) or s is None:
        return 0
    s = str(s)
    hours = re.findall(r"(\d+)H", s)
    minutes = re.findall(r"(\d+)M", s)
    h = int(hours[0]) if hours else 0
    m = int(minutes[0]) if minutes else 0
    return h * 60 + m


def load_data(engine) -> pd.DataFrame:
    """Charge mart.fct_flight_legs (vols non annulés)."""
    query = """
    SELECT * FROM mart.fct_flight_legs
    WHERE cancelled = false
    """
    return pd.read_sql(query, engine)


def prepare_features(df: pd.DataFrame):
    """Prépare X, y et encodeurs."""
    df = df.copy()
    df["scheduled_flight_duration_min"] = df["scheduled_flight_duration_min"].fillna(0)
    for c in NUMERIC_FEATURES:
        if c not in df.columns:
            df[c] = 0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

    df[TARGET] = df[TARGET].astype(bool).astype(int)

    X_num = df[NUMERIC_FEATURES].copy()
    cat_cols = [c for c in CATEGORICAL_FEATURES if c in df.columns]
    if cat_cols:
        ohe = OneHotEncoder(handle_unknown="ignore", sparse_output=False)
        X_cat = ohe.fit_transform(df[cat_cols].fillna("__MISSING__").astype(str))
        X_cat = pd.DataFrame(X_cat, columns=ohe.get_feature_names_out(cat_cols), index=df.index)
        X = pd.concat([X_num, X_cat], axis=1)
        encoders = {"ohe": ohe}
    else:
        X = X_num
        encoders = {}
    y = df[TARGET].values
    return X, y, encoders


def main():
    engine = create_engine(DB_URI)
    df = load_data(engine)
    if df.empty:
        print("Aucune donnée dans mart.fct_flight_legs. Exécuter dbt run avant ml_score.py.")
        return

    X, y, encoders = prepare_features(df)
    ids = df[["leg_id", "flight_id"]].copy()

    X_train, X_test, y_train, y_test, ids_train, ids_test = train_test_split(
        X, y, ids, test_size=0.2, stratify=y, random_state=42
    )

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    X_full_scaled = scaler.transform(X)

    smote = SMOTE(random_state=42, k_neighbors=5)
    X_train_bal, y_train_bal = smote.fit_resample(X_train_scaled, y_train)

    rf = RandomForestClassifier(n_estimators=100, class_weight="balanced", random_state=42)
    rf.fit(X_train_bal, y_train_bal)

    y_pred_test = rf.predict(X_test_scaled)
    y_proba_test = rf.predict_proba(X_test_scaled)[:, 1]

    print("Random Forest - Métriques (test set):")
    print(f"  Accuracy:  {accuracy_score(y_test, y_pred_test):.4f}")
    print(f"  Precision: {precision_score(y_test, y_pred_test, zero_division=0):.4f}")
    print(f"  Recall:    {recall_score(y_test, y_pred_test, zero_division=0):.4f}")
    print(f"  F1:        {f1_score(y_test, y_pred_test, zero_division=0):.4f}")
    print(f"  AUC-ROC:   {roc_auc_score(y_test, y_proba_test):.4f}")

    y_pred_full = rf.predict(X_full_scaled)
    y_proba_full = rf.predict_proba(X_full_scaled)[:, 1]

    scored_full = df.copy()
    scored_full["delay_predicted"] = y_pred_full
    scored_full["delay_probability"] = y_proba_full

    create_sql = """
    CREATE TABLE IF NOT EXISTS public.ml_delays_scored (
        leg_id UUID,
        flight_id VARCHAR(50),
        delay_predicted INTEGER,
        delay_probability FLOAT,
        PRIMARY KEY (leg_id)
    );
    """
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS public.ml_delays_scored CASCADE"))
        conn.execute(text(create_sql))

    cols_out = ["leg_id", "flight_id", "delay_predicted", "delay_probability"]
    scored_full[cols_out].to_sql(
        "ml_delays_scored",
        engine,
        schema="public",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )

    model_dir = Path(__file__).resolve().parent / "models"
    model_dir.mkdir(exist_ok=True)
    joblib.dump(rf, model_dir / "rf_delay_classifier.joblib")
    joblib.dump(scaler, model_dir / "scaler.joblib")
    if encoders:
        joblib.dump(encoders, model_dir / "encoders.joblib")

    print(f"Prédictions écrites dans public.ml_delays_scored ({len(scored_full)} lignes).")


if __name__ == "__main__":
    main()
