"""
EPL S3 Pipeline DAG — Day 21/22

Full pipeline: Kafka → Spark → S3 → Glue Catalog → Athena-ready

Flow:
  check_s3 → spark_transform → verify_s3 → update_glue_catalog → test_athena → summary

Spark ghi trực tiếp S3 qua s3a://.
Glue Catalog tạo bằng boto3 API (free).
Athena query trực tiếp từ Glue tables.

S3 Layout:
  s3://epl-pipeline-processed-nqh/
    processed/epl/matches/season=.../matchday=.../part-*.parquet
    processed/epl/standings/season=.../snapshot_date=.../part-*.parquet
    athena-results/   ← Athena query output
"""

import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator

logger = logging.getLogger(__name__)

S3_BUCKET = os.environ.get("S3_BUCKET_PROCESSED", "epl-pipeline-processed-nqh")
S3_OUTPUT = f"s3a://{S3_BUCKET}/processed/epl"
S3_LOCATION = f"s3://{S3_BUCKET}/processed/epl"  # for Glue (s3:// not s3a://)
ATHENA_OUTPUT = f"s3://{S3_BUCKET}/athena-results/"
GLUE_DATABASE = "epl_db"
TOPICS = ["matches", "events", "standings"]

default_args = {
    "owner": "epl-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="epl_s3_pipeline",
    default_args=default_args,
    description="Kafka → Spark → S3 → Glue → Athena",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["epl", "spark", "s3", "glue", "athena", "day22"],
) as dag:

    # ── Task 1: Check S3 connection ─────────────────────────────
    def check_s3_connection(**context):
        from utils.s3_uploader import S3Uploader
        uploader = S3Uploader(bucket=S3_BUCKET)
        if not uploader.check_bucket_exists():
            raise Exception(f"Cannot access S3 bucket: {S3_BUCKET}")
        logger.info(f"S3 bucket {S3_BUCKET} is accessible")
        return True

    t_check_s3 = ShortCircuitOperator(
        task_id="check_s3_connection",
        python_callable=check_s3_connection,
        provide_context=True,
    )

    # ── Task 2: Spark Transform → S3 directly ──────────────────
    SPARK_SUBMIT_CMD = """
    SPARK_SUBMIT=$(python3 -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'bin', 'spark-submit'))")
    PYSPARK_PYTHON=python3 $SPARK_SUBMIT \
        --master local[*] \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
        --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
        --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp/.ivy2" \
        --conf spark.ui.enabled=false \
        /opt/airflow/src/spark/epl_transformer.py \
        --source kafka \
        --bootstrap-servers kafka:29092 \
        --output {output}
    """.format(output=S3_OUTPUT)

    t_spark = BashOperator(
        task_id="spark_transform",
        bash_command=SPARK_SUBMIT_CMD,
    )

    # ── Task 3: Verify S3 output ───────────────────────────────
    def verify_s3(**context):
        from utils.s3_uploader import S3Uploader
        uploader = S3Uploader(bucket=S3_BUCKET)

        total_objects = 0
        for topic in TOPICS:
            prefix = f"processed/epl/{topic}"
            objects = uploader.list_objects(prefix)
            if objects:
                logger.info(f"  {topic}: {len(objects)} objects on S3")
                total_objects += len(objects)
            else:
                logger.info(f"  {topic}: no objects")

        logger.info(f"Total objects on S3: {total_objects}")
        context["ti"].xcom_push(key="s3_object_count", value=total_objects)

    t_verify = PythonOperator(
        task_id="verify_s3",
        python_callable=verify_s3,
        provide_context=True,
    )

    # ── Task 4: Update Glue Catalog ────────────────────────────
    def update_glue_catalog(**context):
        """Create/update Glue database, tables, and partitions."""
        from utils.glue_catalog import GlueCatalogManager

        glue = GlueCatalogManager(database=GLUE_DATABASE)
        results = glue.setup_all(s3_base=S3_LOCATION)

        logger.info(f"Glue Catalog results: {results}")
        context["ti"].xcom_push(key="glue_results", value=str(results))

    t_glue = PythonOperator(
        task_id="update_glue_catalog",
        python_callable=update_glue_catalog,
        provide_context=True,
    )

    # ── Task 5: Test Athena Query ──────────────────────────────
    def test_athena_query(**context):
        """Run a test query on Athena to verify everything works."""
        import boto3
        import time

        athena = boto3.client("athena", region_name=os.environ.get("AWS_REGION", "ap-southeast-1"))

        # Test query: count matches
        query = f"SELECT COUNT(*) as total_matches FROM {GLUE_DATABASE}.matches"
        logger.info(f"Running Athena query: {query}")

        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": GLUE_DATABASE},
            ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        )
        query_id = response["QueryExecutionId"]
        logger.info(f"Query ID: {query_id}")

        # Wait for query to complete (max 60 seconds)
        for i in range(30):
            result = athena.get_query_execution(QueryExecutionId=query_id)
            state = result["QueryExecution"]["Status"]["State"]

            if state == "SUCCEEDED":
                # Get results
                results = athena.get_query_results(QueryExecutionId=query_id)
                rows = results["ResultSet"]["Rows"]
                if len(rows) > 1:
                    count = rows[1]["Data"][0]["VarCharValue"]
                    logger.info(f"Athena result: {count} total matches")
                    context["ti"].xcom_push(key="athena_match_count", value=count)
                return

            elif state in ("FAILED", "CANCELLED"):
                reason = result["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
                logger.error(f"Athena query {state}: {reason}")
                raise Exception(f"Athena query failed: {reason}")

            time.sleep(2)

        raise Exception("Athena query timeout after 60s")

    t_athena = PythonOperator(
        task_id="test_athena_query",
        python_callable=test_athena_query,
        provide_context=True,
    )

    # ── Task 6: Pipeline Summary ────────────────────────────────
    def pipeline_summary(**context):
        ti = context["ti"]
        s3_count = ti.xcom_pull(task_ids="verify_s3", key="s3_object_count") or 0
        athena_count = ti.xcom_pull(task_ids="test_athena_query", key="athena_match_count") or "N/A"

        logger.info("=" * 60)
        logger.info("  EPL Full Pipeline Summary")
        logger.info(f"  Date: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
        logger.info(f"  S3 Output: {S3_OUTPUT}")
        logger.info(f"  S3 Objects: {s3_count}")
        logger.info(f"  Glue Database: {GLUE_DATABASE}")
        logger.info(f"  Athena match count: {athena_count}")
        logger.info(f"  Pipeline: Kafka -> Spark -> S3 -> Glue -> Athena")
        logger.info("=" * 60)

    t_summary = PythonOperator(
        task_id="pipeline_summary",
        python_callable=pipeline_summary,
        provide_context=True,
    )

    # ── DAG Flow ────────────────────────────────────────────────
    t_check_s3 >> t_spark >> t_verify >> t_glue >> t_athena >> t_summary
