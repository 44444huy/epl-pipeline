import logging
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def create_producer_with_retry(
    bootstrap_servers: str = "localhost:9092",
    max_retries: int = 5,
    retry_delay: int = 5,
) -> KafkaProducer:
    """
    Tạo KafkaProducer với retry — nếu Kafka chưa sẵn sàng thì chờ và thử lại.
    """
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                key_serializer=lambda k: k.encode("utf-8"),

                # Retry config ở cấp độ Kafka client
                retries=3,                    # thử lại 3 lần nếu gửi thất bại
                retry_backoff_ms=500,         # chờ 500ms giữa mỗi lần retry
                request_timeout_ms=30000,     # timeout 30s mỗi request

                # Đảm bảo không mất message
                acks="all",                   # chờ tất cả replicas confirm
                enable_idempotence=True,      # tránh duplicate khi retry
            )
            logger.info(f"✅ Kết nối Kafka thành công (attempt {attempt})")
            return producer

        except NoBrokersAvailable:
            logger.warning(
                f"⚠️  Kafka chưa sẵn sàng. "
                f"Thử lại sau {retry_delay}s... (attempt {attempt}/{max_retries})"
            )
            if attempt < max_retries:
                time.sleep(retry_delay)
            else:
                logger.error("❌ Không thể kết nối Kafka sau nhiều lần thử.")
                raise


def safe_send(
    producer: KafkaProducer,
    topic: str,
    key: str,
    value: bytes,
    dlq_messages: list,
) -> bool:
    """
    Gửi message an toàn. Nếu thất bại → đưa vào Dead Letter Queue (list).
    Trả về True nếu thành công, False nếu thất bại.
    """
    try:
        future = producer.send(topic=topic, key=key, value=value)
        future.get(timeout=10)  # chờ confirm
        return True

    except KafkaError as e:
        logger.error(f"❌ Gửi thất bại topic={topic} key={key}: {e}")
        dlq_messages.append({
            "topic": topic,
            "key": key,
            "value": value,
            "error": str(e),
        })
        return False