from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule
import logging
import os
import sys

sys.path.insert(0, "/opt/airflow")
sys.path.insert(0, "/opt/airflow/src")

from plugins.callbacks.alert_callbacks import (
    on_task_failure,
    on_task_retry,
    on_dag_success,
    on_sla_miss,
)

logger = logging.getLogger(__name__)

default_args = {
    "owner": "epl-pipeline",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,   # 2min, 4min, 8min
    "max_retry_delay": timedelta(minutes=30),
    "email_on_failure": False,
    "on_failure_callback": on_task_failure,
    "on_retry_callback": on_task_retry,
    "execution_timeout": timedelta(minutes=10),  # task timeout
}

with DAG(
    dag_id="epl_robust_pipeline",
    description="EPL pipeline với full error handling",
    schedule_interval="0 */6 * * *",   # mỗi 6 tiếng
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=30),  # DAG timeout
    on_success_callback=on_dag_success,
    sla_miss_callback=on_sla_miss,
    tags=["epl", "kafka", "robust"],
) as dag:

    # ── Task 1: Validate environment ─────────────────────────────
    def validate_environment(**context):
        """Kiểm tra tất cả dependencies trước khi chạy"""
        import os
        from dotenv import load_dotenv
        load_dotenv("/opt/airflow/.env")

        errors = []

        # Check API key
        if not os.getenv("API_FOOTBALL_KEY"):
            errors.append("API_FOOTBALL_KEY chưa được set")

        # Check AWS credentials
        if not os.getenv("AWS_ACCESS_KEY_ID"):
            errors.append("AWS_ACCESS_KEY_ID chưa được set")

        # Check Kafka
        try:
            from kafka import KafkaProducer
            p = KafkaProducer(
                bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "host.docker.internal:9092"),
                request_timeout_ms=5000,
            )
            p.close()
        except Exception as e:
            errors.append(f"Kafka không sẵn sàng: {e}")

        if errors:
            raise ValueError(
                f"Environment validation failed:\n" +
                "\n".join(f"  - {e}" for e in errors)
            )

        logger.info("✅ Environment validation passed")

    t_validate = PythonOperator(
        task_id="validate_environment",
        python_callable=validate_environment,
        sla=timedelta(minutes=2),   # phải xong trong 2 phút
    )

    # ── Task 2: Fetch standings với retry exponential ────────────
    def fetch_standings_robust(**context):
        from dotenv import load_dotenv
        load_dotenv("/opt/airflow/.env")

        from utils.football_api import FootballAPIClient
        from utils.api_mapper import map_standing_to_model
        from utils.kafka_utils import create_producer_with_retry, safe_send_validated

        client = FootballAPIClient()

        try:
            standings = client.get_standings()
        except Exception as e:
            logger.error(f"❌ API call failed: {e}")
            raise   # trigger retry

        if not standings:
            logger.warning("⚠️  Standings rỗng — có thể API đang bảo trì")
            raise ValueError("Empty standings — triggering retry")

        producer = create_producer_with_retry(
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "host.docker.internal:9092")
        )
        dlq = []
        count = 0

        for raw in standings:
            standing = map_standing_to_model(
                raw, snapshot_date=context["ds"]
            )
            if standing:
                safe_send_validated(
                    producer, "epl.standings",
                    standing.team, standing.to_json(), dlq
                )
                count += 1

        producer.flush()
        producer.close()

        if dlq:
            logger.warning(f"⚠️  {len(dlq)} messages failed, going to DLQ")

        logger.info(f"✅ {count} standings published")
        context["ti"].xcom_push(key="standings_count", value=count)
        context["ti"].xcom_push(key="dlq_count", value=len(dlq))

    t_standings = PythonOperator(
        task_id="fetch_standings",
        python_callable=fetch_standings_robust,
        sla=timedelta(minutes=5),
    )

    # ── Task 3: Data quality check ───────────────────────────────
    def check_data_quality(**context):
        """
        Kiểm tra data quality sau khi publish.
        Fail nếu số lượng standings bất thường.
        """
        ti = context["ti"]
        count = ti.xcom_pull(task_ids="fetch_standings", key="standings_count") or 0
        dlq_count = ti.xcom_pull(task_ids="fetch_standings", key="dlq_count") or 0

        # EPL có đúng 20 teams
        if count < 15:
            raise ValueError(
                f"❌ Data quality fail: chỉ có {count} standings, "
                f"expected >= 15 (EPL có 20 teams)"
            )

        if dlq_count > 5:
            raise ValueError(
                f"❌ Too many DLQ messages: {dlq_count}"
            )

        logger.info(f"✅ Data quality OK: {count} standings, {dlq_count} DLQ")

    t_quality = PythonOperator(
        task_id="check_data_quality",
        python_callable=check_data_quality,
        sla=timedelta(minutes=1),
    )

    # ── Task 4: Cleanup / teardown (luôn chạy dù fail) ──────────
    def cleanup(**context):
        """
        Cleanup task — luôn chạy dù pipeline fail.
        Dùng để: log metrics, cleanup temp files, etc.
        """
        ti = context["ti"]
        count = ti.xcom_pull(task_ids="fetch_standings", key="standings_count") or 0

        logger.info(
            f"🏁 Pipeline run complete\n"
            f"   DAG: {context['dag'].dag_id}\n"
            f"   Date: {context['ds']}\n"
            f"   Standings: {count}\n"
            f"   Status: {'SUCCESS' if count > 0 else 'PARTIAL'}"
        )

    t_cleanup = PythonOperator(
        task_id="cleanup",
        python_callable=cleanup,
        trigger_rule=TriggerRule.ALL_DONE,  # luôn chạy dù upstream fail
    )

    # ── Dependencies ─────────────────────────────────────────────
    t_validate >> t_standings >> t_quality >> t_cleanup
# ```

# ---

# ### Phần 5 — Test error scenarios

# **Test 1 — Task timeout:**

# Sửa tạm `execution_timeout=timedelta(seconds=5)` → task sẽ bị kill sau 5 giây → trigger `on_failure_callback` → xem log.

# **Test 2 — Data quality fail:**

# Sửa tạm `if count < 25` → với 20 teams EPL sẽ luôn fail → xem retry với exponential backoff.

# **Test 3 — Cleanup luôn chạy:**

# Dù task nào fail, `cleanup` task với `trigger_rule=ALL_DONE` vẫn chạy — xem trên Graph view.

# ---

# ### Tổng kết Ngày 19
# ```
# ✅ on_failure_callback — alert khi task fail
# ✅ retry_exponential_backoff — retry thông minh
# ✅ execution_timeout — task không bị stuck mãi
# ✅ sla — đảm bảo task xong trong thời gian kỳ vọng
# ✅ TriggerRule.ALL_DONE — cleanup luôn chạy
# ✅ Data quality check — validate output của pipeline