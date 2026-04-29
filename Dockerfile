FROM apache/airflow:3.0.0-python3.12

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc libpq-dev && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

# Création d'un environnement isolé pour dbt
RUN python -m venv /home/airflow/dbt_venv && \
    /home/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-postgres

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r /requirements.txt