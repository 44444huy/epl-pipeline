from datetime import datetime, timedelta
from airflow import DAG
import os

default_args = {
    "owner": "epl-pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="epl_standings_v2",
    description="EPL standings dùng Custom Operator",
    schedule_interval="0 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["epl", "kafka", "standings", "v2"],
) as dag:

    # Import bên trong DAG context — sau khi Airflow đã load xong
    from operators.epl_to_kafka_operator import EPLStandingsToKafkaOperator


    publish_standings = EPLStandingsToKafkaOperator(
        task_id="publish_standings",
        kafka_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "host.docker.internal:9092"),
        topic="epl.standings",
        league_id=39,
        season=2024,
    )