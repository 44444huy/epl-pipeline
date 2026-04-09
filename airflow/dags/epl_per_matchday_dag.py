from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import os
import sys

sys.path.insert(0, "/opt/airflow/src")
logger = logging.getLogger(__name__)

# Config — mỗi item tạo ra 1 DAG riêng
MATCHDAYS = [29, 30, 31, 32, 33]

default_args = {
    "owner": "epl-pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

def create_dag(matchday: int) -> DAG:
    """Factory function — tạo 1 DAG hoàn chỉnh cho 1 vòng đấu"""

    dag = DAG(
        dag_id=f"epl_matchday_{matchday:02d}",
        description=f"EPL Matchday {matchday} pipeline",
        schedule_interval=None,   # trigger thủ công hoặc từ DAG khác
        start_date=datetime(2024, 1, 1),
        catchup=False,
        default_args=default_args,
        tags=["epl", "matchday", f"md{matchday:02d}"],
    )

    def fetch_fixtures(**context):
        from dotenv import load_dotenv
        load_dotenv("/opt/airflow/.env")

        from utils.football_api import FootballAPIClient
        from utils.api_mapper import map_fixture_to_match
        from utils.kafka_utils import create_producer_with_retry, safe_send_validated

        client = FootballAPIClient()
        producer = create_producer_with_retry(
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "host.docker.internal:9092")
        )
        dlq = []
        fixtures = client.get_fixtures_by_matchday(matchday)
        count = 0

        for fixture in fixtures:
            match = map_fixture_to_match(fixture)
            if match:
                safe_send_validated(
                    producer, "epl.matches",
                    match.match_id, match.to_json(), dlq
                )
                count += 1

        producer.flush()
        producer.close()
        logger.info(f"✅ Matchday {matchday}: {count} fixtures")
        context["ti"].xcom_push(key="count", value=count)

    def fetch_events(**context):
        from dotenv import load_dotenv
        load_dotenv("/opt/airflow/.env")

        from utils.football_api import FootballAPIClient
        from utils.api_mapper import map_fixture_to_match, map_event_to_match_event
        from utils.kafka_utils import create_producer_with_retry, safe_send_validated

        client = FootballAPIClient()
        producer = create_producer_with_retry(
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "host.docker.internal:9092")
        )
        dlq = []
        fixtures = client.get_fixtures_by_matchday(matchday)
        event_count = 0

        for fixture in fixtures:
            fixture_id = fixture["fixture"]["id"]
            events = client.get_fixture_events(fixture_id)
            for raw_event in events:
                event = map_event_to_match_event(raw_event, str(fixture_id))
                if event:
                    safe_send_validated(
                        producer, "epl.events",
                        event.match_id, event.to_json(), dlq
                    )
                    event_count += 1

        producer.flush()
        producer.close()
        logger.info(f"✅ Matchday {matchday}: {event_count} events")

    with dag:
        t1 = PythonOperator(
            task_id="fetch_fixtures",
            python_callable=fetch_fixtures,
            provide_context=True,
        )

        t2 = PythonOperator(
            task_id="fetch_events",
            python_callable=fetch_events,
            provide_context=True,
        )

        t1 >> t2

    return dag


# ── Tạo DAGs và đăng ký vào global namespace ────────────────────
# Airflow scan global variables tìm DAG objects
for matchday in MATCHDAYS:
    dag_id = f"epl_matchday_{matchday:02d}"
    globals()[dag_id] = create_dag(matchday)
