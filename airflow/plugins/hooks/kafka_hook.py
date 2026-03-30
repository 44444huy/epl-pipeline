from airflow.hooks.base import BaseHook
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import logging
import time

logger = logging.getLogger(__name__)

class KafkaProducerHook(BaseHook):
    """
    Airflow Hook cho Kafka Producer.
    Quản lý connection lifecycle, tái sử dụng được trong nhiều tasks.
    
    Usage:
        hook = KafkaProducerHook(bootstrap_servers="host.docker.internal:9092")
        with hook.get_producer() as producer:
            producer.send("topic", key=b"key", value=b"value")
    """

    conn_name_attr = "kafka_conn_id"
    default_conn_name = "kafka_default"
    conn_type = "kafka"
    hook_name = "Kafka Producer"

    def __init__(
        self,
        bootstrap_servers: str = "host.docker.internal:9092",
        max_retries: int = 5,
        retry_delay: int = 5,
    ):
        super().__init__()
        self.bootstrap_servers = bootstrap_servers
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._producer = None

    def get_producer(self) -> "KafkaProducerHook":
        """Dùng như context manager: with hook.get_producer() as producer"""
        return self

    def __enter__(self):
        self._producer = self._create_producer()
        return self._producer

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._producer:
            self._producer.flush()
            self._producer.close()
            self._producer = None
        return False

    def _create_producer(self) -> KafkaProducer:
        for attempt in range(1, self.max_retries + 1):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
                    value_serializer=lambda v: v if isinstance(v, bytes) else json.dumps(v).encode("utf-8"),
                    retries=3,
                    retry_backoff_ms=500,
                    request_timeout_ms=30000,
                    acks="all",
                )
                logger.info(f"✅ KafkaProducerHook connected (attempt {attempt})")
                return producer
            except NoBrokersAvailable:
                logger.warning(f"⚠️  Kafka not ready, retry {attempt}/{self.max_retries}")
                if attempt < self.max_retries:
                    time.sleep(self.retry_delay)
                else:
                    raise

    def send_message(self, topic: str, key: str, value: bytes) -> bool:
        """Gửi 1 message, trả về True nếu thành công"""
        if not self._producer:
            raise RuntimeError("Producer chưa được khởi tạo. Dùng context manager.")
        try:
            future = self._producer.send(topic=topic, key=key, value=value)
            future.get(timeout=10)
            return True
        except Exception as e:
            logger.error(f"❌ Send failed topic={topic} key={key}: {e}")
            return False