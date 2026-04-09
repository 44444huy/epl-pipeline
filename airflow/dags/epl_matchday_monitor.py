from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
import logging
import os
import sys

sys.path.insert(0, "/opt/airflow/src")
logger = logging.getLogger(__name__)

default_args = {
    "owner": "epl-pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
}

with DAG(
    dag_id="epl_matchday_monitor",
    description="Monitor EPL live scores mỗi 15 phút",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,      # chỉ 1 run tại 1 thời điểm
    default_args=default_args,
    tags=["epl", "kafka", "realtime"],
) as dag:

    # ShortCircuit — dừng pipeline nếu điều kiện không thỏa
    def check_live_matches(**context):
        """Trả về True nếu có trận đang diễn ra, False để skip"""
        import requests
        import os
        from dotenv import load_dotenv
        load_dotenv("/opt/airflow/.env")

        api_key = os.getenv("API_FOOTBALL_KEY")
        response = requests.get(
            "https://v3.football.api-sports.io/fixtures",
            headers={"x-apisports-key": api_key},
            params={"league": 39, "live": "all"},
            timeout=10,
        )
        fixtures = response.json().get("response", [])
        count = len(fixtures)
        logger.info(f"📡 {count} trận EPL đang live")

        context["ti"].xcom_push(key="live_count", value=count)
        return count > 0   # False → skip tất cả tasks tiếp theo

    t_check_live = ShortCircuitOperator(
        task_id="check_live_matches",
        python_callable=check_live_matches,
        provide_context=True,
    )

    def publish_live_scores(**context):
        import requests
        import os
        from dotenv import load_dotenv
        load_dotenv("/opt/airflow/.env")

        from utils.api_mapper import map_fixture_to_match, map_event_to_match_event
        from utils.kafka_utils import create_producer_with_retry, safe_send_validated

        api_key = os.getenv("API_FOOTBALL_KEY")
        season = os.getenv("EPL_SEASON", "2024")

        response = requests.get(
            "https://v3.football.api-sports.io/fixtures",
            headers={"x-apisports-key": api_key},
            params={"league": 39, "live": "all"},
            timeout=10,
        )
        fixtures = response.json().get("response", [])

        producer = create_producer_with_retry(
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "host.docker.internal:9092")
        )
        dlq = []
        match_count = 0

        for fixture in fixtures:
            match = map_fixture_to_match(fixture)
            if match:
                safe_send_validated(
                    producer, "epl.matches",
                    match.match_id, match.to_json(), dlq
                )
                match_count += 1
                logger.info(
                    f"⚽ {match.home_team} {match.home_score}"
                    f"-{match.away_score} {match.away_team}"
                )

        producer.flush()
        producer.close()
        logger.info(f"✅ Published {match_count} live match scores")

    t_publish_live = PythonOperator(
        task_id="publish_live_scores",
        python_callable=publish_live_scores,
        provide_context=True,
    )

    t_check_live >> t_publish_live
# ```

# ---

# ### Phần 4 — Trigger và quan sát

# Vào **http://localhost:8081**:
# ```
# 1. Toggle ON cả 3 DAGs
# 2. Trigger manual epl_daily_pipeline
# 3. Vào Graph view quan sát flow:
#    - check_kafka + check_api chạy song song
#    - branch_has_fixtures quyết định đi nhánh nào
#    - fetch_standings chạy song song với fixtures
#    - pipeline_summary chờ tất cả xong
# ```

# **Điều thú vị cần quan sát:**

# `BranchPythonOperator` — task tự chọn đường đi dựa theo logic:
# ```
# Hôm nay có trận → fetch_todays_fixtures 🟢, no_fixtures_today ⬜ (skipped)
# Hôm nay không có → fetch_todays_fixtures ⬜ (skipped), no_fixtures_today 🟢
# ```

# `ShortCircuitOperator` — dừng toàn bộ pipeline nếu không có live matches:
# ```
# Không có trận live → check_live_matches 🟢, publish_live_scores ⬜ (skipped)