from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
import logging
import os
import sys

sys.path.insert(0, "/opt/airflow/src")
logger = logging.getLogger(__name__)

default_args = {
    "owner": "epl-pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

with DAG(
    dag_id="epl_daily_pipeline",
    description="EPL daily pipeline — fetch fixtures, standings, publish Kafka",
    schedule_interval="0 6 * * *",   # 6AM UTC mỗi ngày
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["epl", "kafka", "daily"],
) as dag:

    # ── Task 1: Check Kafka health ───────────────────────────────
    def check_kafka(**context):
        from kafka import KafkaProducer
        try:
            kafka_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "host.docker.internal:9092")
            p = KafkaProducer(bootstrap_servers=kafka_servers)
            p.close()
            logger.info("✅ Kafka OK")
        except Exception as e:
            raise Exception(f"❌ Kafka không sẵn sàng: {e}")

    t_check_kafka = PythonOperator(
        task_id="check_kafka",
        python_callable=check_kafka,
    )

    # ── Task 2: Check API available ──────────────────────────────
    def check_api(**context):
        import requests
        import os
        from dotenv import load_dotenv
        load_dotenv("/opt/airflow/.env")

        api_key = os.getenv("API_FOOTBALL_KEY")
        response = requests.get(
            "https://v3.football.api-sports.io/status",
            headers={"x-apisports-key": api_key},
            timeout=10,
        )
        if response.status_code != 200:
            raise Exception(f"API không sẵn sàng: {response.status_code}")

        data = response.json()
        logger.info(f"✅ API OK")
        return True

    t_check_api = PythonOperator(
        task_id="check_api",
        python_callable=check_api,
    )

    # ── Task 3: Branch — có trận hôm nay không? ─────────────────
    def branch_has_fixtures(**context):
        import requests
        import os
        from dotenv import load_dotenv
        load_dotenv("/opt/airflow/.env")

        today = context['ds']
        api_key = os.getenv("API_FOOTBALL_KEY")
        season = os.getenv("EPL_SEASON", "2024")

        response = requests.get(
            "https://v3.football.api-sports.io/fixtures",
            headers={"x-apisports-key": api_key},
            params={"league": 39, "season": season, "date": today},
            timeout=10,
        )
        fixtures = response.json().get("response", [])
        count = len(fixtures)
        logger.info(f"📅 Hôm nay có {count} trận EPL")

        # Push vào XCom để task sau dùng
        context["ti"].xcom_push(key="fixture_count", value=count)

        if count > 0:
            return "fetch_todays_fixtures"
        else:
            return "no_fixtures_today"

    t_branch = BranchPythonOperator(
        task_id="check_todays_fixtures",
        python_callable=branch_has_fixtures,
        provide_context=True,
    )

    # ── Task 4a: Có trận → fetch và publish ─────────────────────
    def fetch_todays_fixtures(**context):
        import requests
        import os
        from dotenv import load_dotenv
        load_dotenv("/opt/airflow/.env")

        sys.path.insert(0, "/opt/airflow/src")
        from utils.api_mapper import map_fixture_to_match
        from utils.kafka_utils import create_producer_with_retry, safe_send_validated

        today = context['ds']
        api_key = os.getenv("API_FOOTBALL_KEY")
        season = os.getenv("EPL_SEASON", "2024")

        response = requests.get(
            "https://v3.football.api-sports.io/fixtures",
            headers={"x-apisports-key": api_key},
            params={"league": 39, "season": season, "date": today},
            timeout=10,
        )
        fixtures = response.json().get("response", [])

        producer = create_producer_with_retry(
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "host.docker.internal:9092")
        )
        dlq = []
        count = 0

        for fixture in fixtures:
            match = map_fixture_to_match(fixture)
            if match:
                safe_send_validated(
                    producer, "epl.matches",
                    match.match_id, match.to_json(), dlq
                )
                count += 1
                logger.info(f"⚽ {match.home_team} vs {match.away_team} [{match.status}]")

        producer.flush()
        producer.close()
        logger.info(f"✅ Published {count} fixtures")
        context["ti"].xcom_push(key="published_count", value=count)

    t_fetch_fixtures = PythonOperator(
        task_id="fetch_todays_fixtures",
        python_callable=fetch_todays_fixtures,
        provide_context=True,
    )

    # ── Task 4b: Không có trận ───────────────────────────────────
    t_no_fixtures = EmptyOperator(
        task_id="no_fixtures_today",
    )

    # ── Task 5: Fetch standings (chạy song song với fixtures) ────
    def fetch_standings(**context):
        """
            Fetch EPL standings và publish lên Kafka.
            
            API Football không support standings theo date.
            Standings luôn là snapshot real-time tại thời điểm chạy.
            Backfill DAG này sẽ luôn lấy standings hiện tại, không phải
            standings của ngày execution_date.
        """
        from dotenv import load_dotenv
        load_dotenv("/opt/airflow/.env")

        from utils.football_api import FootballAPIClient
        from utils.api_mapper import map_standing_to_model
        from utils.kafka_utils import create_producer_with_retry, safe_send_validated

        client = FootballAPIClient()
        producer = create_producer_with_retry(
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "host.docker.internal:9092")
        )
        dlq = []

        standings = client.get_standings()
        count = 0
        for raw in standings:
            standing = map_standing_to_model(raw)
            if standing:
                safe_send_validated(
                    producer, "epl.standings",
                    standing.team, standing.to_json(), dlq
                )
                count += 1

        producer.flush()
        producer.close()
        logger.info(f"✅ Published {count} standings")
        context["ti"].xcom_push(key="standings_count", value=count)

    t_fetch_standings = PythonOperator(
        task_id="fetch_standings",
        python_callable=fetch_standings,
        provide_context=True,
    )

    # ── Task 6: Summary ─────────────────────────────────────────
    def pipeline_summary(**context):
        ti = context["ti"]

        fixture_count = ti.xcom_pull(
            task_ids="check_todays_fixtures", key="fixture_count"
        ) or 0
        standings_count = ti.xcom_pull(
            task_ids="fetch_standings", key="standings_count"
        ) or 0

        logger.info("=" * 40)
        logger.info(f"📊 EPL Daily Pipeline Summary")
        logger.info(f"   Fixtures hôm nay: {fixture_count}")
        logger.info(f"   Standings published: {standings_count}")
        logger.info("=" * 40)

    t_summary = PythonOperator(
        task_id="pipeline_summary",
        python_callable=pipeline_summary,
        provide_context=True,
        trigger_rule="none_failed_min_one_success",
    )

    # ── Dependencies ─────────────────────────────────────────────
    #
    #  check_kafka ──┬──> check_api ──> check_todays_fixtures
    #                │                        │
    #                │              ┌──────────┴──────────┐
    #                │              ▼                      ▼
    #                │    fetch_todays_fixtures    no_fixtures_today
    #                │              │                      │
    #                │              └──────────┬───────────┘
    #                │                         ▼
    #                └──────────────> fetch_standings
    #                                          │
    #                                          ▼
    #                                   pipeline_summary

    [t_check_kafka, t_check_api] >> t_branch
    t_branch >> [t_fetch_fixtures, t_no_fixtures]
    [t_fetch_fixtures, t_no_fixtures] >> t_summary
    t_check_kafka >> t_fetch_standings >> t_summary