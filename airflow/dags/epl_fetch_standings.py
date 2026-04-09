from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import os
import sys

sys.path.insert(0, "/opt/airflow/src")
logger = logging.getLogger(__name__)

default_args = {
    "owner": "epl-pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="epl_fetch_standings",
    description="Fetch EPL standings → publish Kafka mỗi giờ",
    schedule_interval="0 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["epl", "kafka", "standings"],
) as dag:

    def check_kafka(**context):
        from kafka import KafkaProducer
        try:
            kafka_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "host.docker.internal:9092")
            p = KafkaProducer(bootstrap_servers=kafka_servers)
            p.close()
            logger.info("✅ Kafka OK")
        except Exception as e:
            raise Exception(f"❌ Kafka không sẵn sàng: {e}")

    def fetch_and_publish(**context):
        import os
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

        try:
            standings = client.get_standings()
            if not standings:
                logger.warning("⚠️  Không có standings data")
                return

            count = 0
            for raw in standings:
                standing = map_standing_to_model(raw,snapshot_date=context['ds'])
                if standing:
                    safe_send_validated(
                        producer, "epl.standings",
                        standing.team, standing.to_json(), dlq
                    )
                    count += 1

            producer.flush()
            logger.info(f"✅ Published {count} standings")
            context["ti"].xcom_push(key="count", value=count)

        finally:
            producer.close()
            if dlq:
                logger.warning(f"⚠️  DLQ: {len(dlq)} messages")

    def log_result(**context):
        count = context["ti"].xcom_pull(
            task_ids="fetch_and_publish",
            key="count"
        )
        logger.info(f"📊 Done — {count} standings published to Kafka")

    t1 = PythonOperator(
        task_id="check_kafka",
        python_callable=check_kafka,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id="fetch_and_publish",
        python_callable=fetch_and_publish,
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id="log_result",
        python_callable=log_result,
        provide_context=True,
    )

    t1 >> t2 >> t3

