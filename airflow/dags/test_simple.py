import logging
from datetime import datetime
from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

@dag(
    dag_id="test_airflow_basique",
    start_date=datetime(2026, 4, 20),
    schedule=None,
    catchup=False,
)
def test_simple_dag():

    @task
    def say_hello():
        logger.info("=========================================")
        logger.info("Tâche OK ! Le LocalExecutor fonctionne !")
        logger.info("=========================================")
        return "Succès"

    say_hello()

test_simple_dag()