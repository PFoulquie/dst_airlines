import os
from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

# Import de Cosmos
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

# --- CONFIGURATION DBT ---
DBT_PATH = "/opt/airflow/dbt"

profile_config = ProfileConfig(
    profile_name="afklm_pipeline",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="supabase_conn",
        profile_args={"schema": "silver"},
    ),
)

@dag(
    dag_id='afklm_full_pipeline',
    start_date=datetime(2026, 4, 20),
    schedule='@daily',
    catchup=False,
    tags=['production', 'afklm']
)
def afklm_full_pipeline():
    
    start = EmptyOperator(task_id="start")

    @task
    def ingestion_bronze(ds=None):
        # Correction ici : on importe le vrai nom de la fonction
        from b_afklm_flight import extract_and_load_bronze 
        return extract_and_load_bronze(ds)

    # Cosmos transforme automatiquement tes .sql en tâches Airflow
    silver_layer = DbtTaskGroup(
        group_id="silver_layer",
        project_config=ProjectConfig(DBT_PATH),
        profile_config=profile_config,
        render_config=RenderConfig(
            select=["path:models/silver"]
        )
    )

    end = EmptyOperator(task_id="end")

    # Orchestration
    start >> ingestion_bronze() >> silver_layer >> end

afklm_full_pipeline()