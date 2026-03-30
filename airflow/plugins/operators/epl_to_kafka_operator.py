from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging
import sys

logger = logging.getLogger(__name__)

class EPLStandingsToKafkaOperator(BaseOperator):
    """
    Custom Operator: Fetch EPL standings → publish to Kafka.
    
    Đóng gói toàn bộ logic vào 1 operator tái sử dụng.
    
    Usage trong DAG:
        publish_standings = EPLStandingsToKafkaOperator(
            task_id="publish_standings",
            kafka_servers="host.docker.internal:9092",
            topic="epl.standings",
            league_id=39,
            season=2024,
        )
    """

    template_fields = ("season",)   # cho phép Jinja template

    @apply_defaults
    def __init__(
        self,
        kafka_servers: str = "host.docker.internal:9092",
        topic: str = "epl.standings",
        league_id: int = 39,
        season: int = 2024,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.kafka_servers = kafka_servers
        self.topic = topic
        self.league_id = league_id
        self.season = season

    def execute(self, context):
        sys.path.insert(0, "/opt/airflow/src")
        from dotenv import load_dotenv
        load_dotenv("/opt/airflow/.env")

        from hooks.kafka_hook import KafkaProducerHook
        from hooks.football_api_hook import FootballAPIHook
        from utils.api_mapper import map_standing_to_model
        from utils.kafka_utils import validate_message

        api_hook = FootballAPIHook()
        kafka_hook = KafkaProducerHook(bootstrap_servers=self.kafka_servers)

        standings = api_hook.get_standings(
            league_id=self.league_id,
            season=self.season,
        )

        if not standings:
            logger.warning("⚠️  Không có standings data")
            return 0

        published = 0
        failed = 0

        with kafka_hook.get_producer() as producer:
            for raw in standings:
                standing = map_standing_to_model(
                    raw,
                    snapshot_date=context["ds"]
                )
                if not standing:
                    continue

                value = standing.to_json()
                is_valid, err = validate_message(self.topic, value)

                if not is_valid:
                    logger.error(f"❌ Schema invalid: {err}")
                    failed += 1
                    continue

                success = kafka_hook.send_message(
                    topic=self.topic,
                    key=standing.team,
                    value=value,
                )
                if success:
                    published += 1
                else:
                    failed += 1

        logger.info(f"✅ Published {published} standings, failed {failed}")

        # Push XCom
        context["ti"].xcom_push(key="published_count", value=published)
        context["ti"].xcom_push(key="failed_count", value=failed)

        return published