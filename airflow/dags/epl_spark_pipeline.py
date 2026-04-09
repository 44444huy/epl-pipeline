"""
DAG: epl_spark_pipeline — Day 20

Airflow trigger Spark job để transform EPL data.

Flow:
  check_kafka → check_data_exists → spark_transform → verify_output

2 cách trigger Spark từ Airflow:
  1. BashOperator — spark-submit command (đơn giản, dùng cho local/Docker)
  2. SparkSubmitOperator — native operator (dùng cho Spark cluster)

Ở đây dùng BashOperator vì Spark chạy cùng Docker network.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator
import logging
import os
import sys

sys.path.insert(0, "/opt/airflow/src")
logger = logging.getLogger(__name__)

KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "host.docker.internal:9092")

default_args = {
    "owner": "epl-pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
    "execution_timeout": timedelta(minutes=15),
}

with DAG(
    dag_id="epl_spark_pipeline",
    description="Trigger Spark job to transform EPL data (Kafka → Parquet)",
    schedule_interval="0 */6 * * *",  # mỗi 6 tiếng
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["epl", "spark", "transform"],
) as dag:

    # ── Task 1: Check Kafka có data không ────────────────────────
    def check_data_exists(**context):
        """
        Kiểm tra Kafka topics có data trước khi chạy Spark.
        Tránh chạy Spark job tốn resource mà không có data.
        """
        from kafka import KafkaConsumer, TopicPartition

        topics = ["epl.matches", "epl.events", "epl.standings"]
        total_messages = 0

        try:
            consumer = KafkaConsumer(
                bootstrap_servers=KAFKA_SERVERS,
                request_timeout_ms=10000,
            )

            for topic in topics:
                partitions = consumer.partitions_for_topic(topic)
                if not partitions:
                    logger.warning(f"Topic {topic} không tồn tại hoặc chưa có partition")
                    continue

                for p in partitions:
                    tp = TopicPartition(topic, p)
                    consumer.assign([tp])
                    consumer.seek_to_end(tp)
                    end_offset = consumer.position(tp)
                    consumer.seek_to_beginning(tp)
                    start_offset = consumer.position(tp)
                    count = end_offset - start_offset
                    total_messages += count
                    logger.info(f"  {topic}[{p}]: {count} messages")

            consumer.close()

        except Exception as e:
            logger.error(f"Kafka check failed: {e}")
            raise

        logger.info(f"Total messages across all topics: {total_messages}")
        context["ti"].xcom_push(key="total_messages", value=total_messages)

        return total_messages > 0  # False → skip Spark job

    t_check_data = ShortCircuitOperator(
        task_id="check_data_exists",
        python_callable=check_data_exists,
        provide_context=True,
    )

    # ── Task 2: Spark transform (BashOperator) ───────────────────
    #
    # BashOperator gọi spark-submit trên Spark container.
    # Dùng docker exec vì Airflow và Spark ở cùng Docker network.
    #
    # Trong production thật:
    #   - Dùng SparkSubmitOperator kết nối Spark cluster
    #   - Hoặc dùng Livy REST API (LivyOperator)
    #   - Hoặc EMR trên AWS (EmrAddStepsOperator)
    #
    # Chạy PySpark local mode trực tiếp trong Airflow container
    # Không cần Spark cluster riêng — local[*] dùng tất cả CPU cores có sẵn
    # Trong production thật sẽ dùng SparkSubmitOperator kết nối EMR/Databricks
    # spark-submit cài qua pip nằm trong thư mục pyspark, không có trong $PATH
    # Dùng python -c để lấy đúng path trước khi chạy
    SPARK_SUBMIT_CMD = """
    SPARK_SUBMIT=$(python3 -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'bin', 'spark-submit'))")
    PYSPARK_PYTHON=python3 $SPARK_SUBMIT \
        --master local[*] \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp/.ivy2" \
        --conf spark.ui.enabled=false \
        /opt/airflow/src/spark/epl_transformer.py \
        --source kafka \
        --bootstrap-servers kafka:29092 \
        --output /tmp/epl-spark-output
    """

    t_spark_transform = BashOperator(
        task_id="spark_transform",
        bash_command=SPARK_SUBMIT_CMD,
        execution_timeout=timedelta(minutes=10),
    )

    # ── Task 3: Verify output ────────────────────────────────────
    def verify_spark_output(**context):
        import os
        output_base = "/tmp/epl-spark-output"

        if not os.path.exists(output_base):
            raise Exception(f"Output directory not found: {output_base}")

        found = os.listdir(output_base)
        logger.info(f"Spark output directories: {found}")

        for name in ["matches", "events", "standings"]:
            path = os.path.join(output_base, name)
            if os.path.exists(path):
                parquet_files = []
                for root, dirs, files in os.walk(path):
                    parquet_files.extend(f for f in files if f.endswith(".parquet"))
                logger.info(f"  ✅ {name}/: {len(parquet_files)} parquet files")
            else:
                logger.warning(f"  ⚠️ {name}/ missing — có thể chưa có data")

        context["ti"].xcom_push(key="output_listing", value=str(found))

    t_verify = PythonOperator(
        task_id="verify_spark_output",
        python_callable=verify_spark_output,
        provide_context=True,
    )

    # ── Task 4: Log summary ──────────────────────────────────────
    def log_summary(**context):
        ti = context["ti"]
        total = ti.xcom_pull(task_ids="check_data_exists", key="total_messages") or 0
        logger.info("=" * 50)
        logger.info(f"  EPL Spark Pipeline Summary")
        logger.info(f"  Date: {context['ds']}")
        logger.info(f"  Kafka messages processed: {total}")
        logger.info("=" * 50)

    t_summary = PythonOperator(
        task_id="pipeline_summary",
        python_callable=log_summary,
        provide_context=True,
    )

    # ── Dependencies ─────────────────────────────────────────────
    #
    #  check_data_exists → spark_transform → verify_output → summary
    #
    t_check_data >> t_spark_transform >> t_verify >> t_summary
