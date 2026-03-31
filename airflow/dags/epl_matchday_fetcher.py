from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import sys

sys.path.insert(0, "/opt/airflow/src")
logger = logging.getLogger(__name__)

# ── Config các vòng đấu cần fetch ───────────────────────────────
# Trong thực tế có thể đọc từ DB hoặc config file
MATCHDAYS_CONFIG = [
    {"matchday": 29, "date": "2025-03-08"},
    {"matchday": 30, "date": "2025-03-15"},
    {"matchday": 31, "date": "2025-03-29"},
    {"matchday": 32, "date": "2025-04-05"},
    {"matchday": 33, "date": "2025-04-12"},
]

def make_fetch_task(dag, matchday: int, matchday_date: str):
    """Factory function tạo task cho từng vòng đấu"""

    def fetch_matchday_fixtures(**context):
        from dotenv import load_dotenv
        load_dotenv("/opt/airflow/.env")

        from utils.football_api import FootballAPIClient
        from utils.api_mapper import map_fixture_to_match
        from utils.kafka_utils import create_producer_with_retry, safe_send_validated

        client = FootballAPIClient()
        producer = create_producer_with_retry(
            bootstrap_servers="host.docker.internal:9092"
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
                logger.info(
                    f"[Matchday {matchday}] "
                    f"{match.home_team} vs {match.away_team} [{match.status}]"
                )

        producer.flush()
        producer.close()
        logger.info(f"✅ Matchday {matchday}: {count} fixtures published")

        context["ti"].xcom_push(key="count", value=count)
        return count

    return PythonOperator(
        task_id=f"fetch_matchday_{matchday}",
        python_callable=fetch_matchday_fixtures,
        provide_context=True,
        dag=dag,
    )


def make_summary_task(dag, matchdays: list):
    """Task tổng kết sau khi fetch xong tất cả vòng"""

    def summarize(**context):
        ti = context["ti"]
        total = 0
        for config in matchdays:
            count = ti.xcom_pull(
                task_ids=f"fetch_matchday_{config['matchday']}",
                key="count",
            ) or 0
            total += count
            logger.info(f"  Matchday {config['matchday']}: {count} fixtures")
        logger.info(f"✅ Total published: {total} fixtures")

    return PythonOperator(
        task_id="summarize",
        python_callable=summarize,
        provide_context=True,
        dag=dag,
    )


# ── Tạo DAG động ────────────────────────────────────────────────
default_args = {
    "owner": "epl-pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

with DAG(
    dag_id="epl_matchday_fetcher",
    description="Fetch fixtures cho các vòng đấu EPL",
    schedule_interval="0 0 * * 1",   # mỗi thứ 2
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["epl", "kafka", "matchday", "dynamic"],
) as dag:

    # Tạo tasks động cho từng vòng
    fetch_tasks = [
        make_fetch_task(dag, c["matchday"], c["date"])
        for c in MATCHDAYS_CONFIG
    ]

    summary_task = make_summary_task(dag, MATCHDAYS_CONFIG)

    # Dependencies — tất cả fetch tasks chạy song song, sau đó summary
    fetch_tasks >> summary_task