"""
EPL S3 Pipeline DAG — Day 21-23

Full pipeline: Kafka → Spark → S3 → Glue Catalog → Athena → Data Quality

Flow:
  check_s3 → spark_transform → verify_s3 → update_glue_catalog
    → data_quality_checks → test_athena → summary

Spark ghi trực tiếp S3 qua s3a://.
Glue Catalog tạo bằng boto3 API (free).
Athena query trực tiếp từ Glue tables.
Data quality checks validate row counts, duplicates, freshness, schema.

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
    tags=["epl", "spark", "s3", "glue", "athena", "dq", "day23"],
) as dag:

    # ── Task 1: Check Kafka connectivity ──────────────────────────
    def check_kafka_data(**context):
        """Check Kafka broker is reachable. Data should exist from backfill DAG."""
        import socket

        host, port = "kafka", 29092
        try:
            sock = socket.create_connection((host, port), timeout=10)
            sock.close()
            logger.info(f"Kafka broker {host}:{port} is reachable")
        except Exception as e:
            raise Exception(f"Cannot connect to Kafka {host}:{port}: {e}")

        logger.info("Ensure backfill DAG has been run before this pipeline!")
        context["ti"].xcom_push(key="kafka_check", value="reachable")

    t_check_kafka = PythonOperator(
        task_id="check_kafka_data",
        python_callable=check_kafka_data,
        provide_context=True,
    )

    # ── Task 2: Check S3 connection ─────────────────────────────
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

    # ── Task 3: Spark Transform → S3 directly ──────────────────
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

    # ── Task 5: Data Quality Checks ──────────────────────────────
    # NOTE: Silver views (v_matches, v_standings) đã được chuyển sang dbt.
    # dbt run sau DAG này để tạo Silver + Gold layer.
    def data_quality_checks(**context):
        """Run data quality checks via Athena: row counts, duplicates, freshness, schema."""
        from utils.athena_queries import AthenaQueryManager

        aq = AthenaQueryManager(
            database=GLUE_DATABASE,
            output_location=ATHENA_OUTPUT,
        )

        checks = aq.run_all_checks(min_matches=1, max_age_days=7)
        context["ti"].xcom_push(key="dq_results", value=str(checks))
        context["ti"].xcom_push(key="dq_passed", value=checks["all_passed"])
        context["ti"].xcom_push(key="dq_cost", value=str(checks["cost"]))

        if not checks["all_passed"]:
            failed = [
                name for name, check in checks.items()
                if isinstance(check, dict) and not check.get("passed", check.get("all_passed", True))
            ]
            logger.warning(f"Data quality checks FAILED: {failed}")
            # Don't raise — let pipeline continue but flag the issue
        else:
            logger.info("All data quality checks PASSED")

    t_dq = PythonOperator(
        task_id="data_quality_checks",
        python_callable=data_quality_checks,
        provide_context=True,
    )

    # ── Task 7: Athena Analytics Queries ───────────────────────
    def test_athena_analytics(**context):
        """Run analytics queries on Silver views (deduped data)."""
        from utils.athena_queries import AthenaQueryManager

        aq = AthenaQueryManager(
            database=GLUE_DATABASE,
            output_location=ATHENA_OUTPUT,
        )

        # Count matches
        r = aq.count_matches(season="2024/25")
        match_count = r["rows"][0]["total"] if r["rows"] else "0"
        logger.info(f"Total matches: {match_count}")
        context["ti"].xcom_push(key="athena_match_count", value=match_count)

        # Home vs Away stats
        r = aq.get_home_vs_away_stats(season="2024/25")
        for row in r["rows"]:
            logger.info(f"  {row['result']}: {row['match_count']} ({row['percentage']}%)")

        # League table (top 5)
        r = aq.get_league_table(season="2024/25")
        for row in r["rows"][:5]:
            logger.info(f"  #{row['rank']} {row['team']} - {row['points']} pts")

        # Cost summary
        cost = aq.get_cost_summary()
        logger.info(
            f"Athena cost: {cost['query_count']} queries, "
            f"{cost['total_mb_scanned']:.2f} MB scanned, "
            f"~${cost['estimated_cost_usd']:.6f}"
        )
        context["ti"].xcom_push(key="athena_cost", value=str(cost))

    t_athena = PythonOperator(
        task_id="test_athena_analytics",
        python_callable=test_athena_analytics,
        provide_context=True,
    )

    # ── Task 8: Pipeline Summary ────────────────────────────────
    def pipeline_summary(**context):
        ti = context["ti"]
        s3_count = ti.xcom_pull(task_ids="verify_s3", key="s3_object_count") or 0
        athena_count = ti.xcom_pull(task_ids="test_athena_analytics", key="athena_match_count") or "N/A"
        dq_passed = ti.xcom_pull(task_ids="data_quality_checks", key="dq_passed")
        dq_cost = ti.xcom_pull(task_ids="data_quality_checks", key="dq_cost") or "N/A"
        athena_cost = ti.xcom_pull(task_ids="test_athena_analytics", key="athena_cost") or "N/A"

        logger.info("=" * 60)
        logger.info("  EPL Full Pipeline Summary")
        logger.info(f"  Date: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
        logger.info(f"  S3 Output: {S3_OUTPUT}")
        logger.info(f"  S3 Objects: {s3_count}")
        logger.info(f"  Glue Database: {GLUE_DATABASE}")
        logger.info(f"  Athena match count: {athena_count}")
        logger.info(f"  Data Quality: {'ALL PASSED' if dq_passed else 'SOME FAILED'}")
        logger.info(f"  DQ Cost: {dq_cost}")
        logger.info(f"  Analytics Cost: {athena_cost}")
        logger.info(f"  Pipeline: Kafka -> Spark -> S3 -> Glue -> Views -> DQ -> Athena")
        logger.info("=" * 60)

    t_summary = PythonOperator(
        task_id="pipeline_summary",
        python_callable=pipeline_summary,
        provide_context=True,
    )

    # ── DAG Flow ────────────────────────────────────────────────
    # Producer + S3 check run in parallel, then Spark reads from Kafka
    [t_check_kafka, t_check_s3] >> t_spark >> t_verify >> t_glue >> t_dq >> t_athena >> t_summary
