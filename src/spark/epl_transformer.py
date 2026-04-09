"""
EPL Spark Transformer — Day 20/22

Đọc EPL data từ Kafka topics (JSON), transform, ghi ra Parquet.
Hỗ trợ ghi ra local hoặc trực tiếp S3 (s3a://).

3 jobs:
  1. matches   → cleaned + enriched → parquet
  2. events    → cleaned + aggregated → parquet
  3. standings → deduplicated → parquet

Chạy local:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    epl_transformer.py --output /tmp/epl-spark-output

Chạy ghi S3:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
    org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    epl_transformer.py --output s3a://bucket/processed/epl
"""

import argparse
import os
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType
)


# ── Schemas ──────────────────────────────────────────────────────

MATCH_SCHEMA = StructType([
    StructField("match_id", StringType(), False),
    StructField("home_team", StringType(), False),
    StructField("away_team", StringType(), False),
    StructField("home_score", IntegerType(), False),
    StructField("away_score", IntegerType(), False),
    StructField("status", StringType(), False),
    StructField("matchday", IntegerType(), False),
    StructField("season", StringType(), False),
    StructField("venue", StringType(), False),
    StructField("timestamp", StringType(), False),
])

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("match_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("minute", IntegerType(), False),
    StructField("team", StringType(), False),
    StructField("player", StringType(), False),
    StructField("detail", StringType(), True),
    StructField("timestamp", StringType(), False),
])

STANDING_SCHEMA = StructType([
    StructField("team", StringType(), False),
    StructField("rank", IntegerType(), False),
    StructField("played", IntegerType(), False),
    StructField("won", IntegerType(), False),
    StructField("drawn", IntegerType(), False),
    StructField("lost", IntegerType(), False),
    StructField("goals_for", IntegerType(), False),
    StructField("goals_against", IntegerType(), False),
    StructField("goal_diff", IntegerType(), False),
    StructField("points", IntegerType(), False),
    StructField("season", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("snapshot_date", StringType(), True),
])


def create_spark_session(app_name: str = "EPL-Transformer", output_path: str = "") -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )

    # S3 config khi output là s3a://
    if output_path.startswith("s3a://"):
        aws_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
        aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
        aws_region = os.environ.get("AWS_REGION", "ap-southeast-1")

        builder = (
            builder
            .config("spark.hadoop.fs.s3a.access.key", aws_key)
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret)
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.path.style.access", "false")
        )

    return builder.getOrCreate()


# ── Transform functions ──────────────────────────────────────────

def transform_matches(df: DataFrame) -> DataFrame:
    """
    Matches transform:
    - Parse timestamp → proper TimestampType
    - Add total_goals column
    - Add result column (home_win / away_win / draw)
    - Deduplicate: keep latest record per match_id
    """
    return (
        df
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .withColumn("total_goals", F.col("home_score") + F.col("away_score"))
        .withColumn(
            "result",
            F.when(F.col("home_score") > F.col("away_score"), "home_win")
            .when(F.col("home_score") < F.col("away_score"), "away_win")
            .otherwise("draw")
        )
        .withColumn(
            "row_num",
            F.row_number().over(
                Window.partitionBy("match_id").orderBy(F.col("event_time").desc())
            )
        )
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )


def transform_events(df: DataFrame) -> DataFrame:
    """
    Events transform:
    - Parse timestamp
    - Deduplicate by event_id
    - Add half column (1st half / 2nd half / extra time)
    """
    return (
        df
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .withColumn(
            "half",
            F.when(F.col("minute") <= 45, "1st_half")
            .when(F.col("minute") <= 90, "2nd_half")
            .otherwise("extra_time")
        )
        .dropDuplicates(["event_id"])
    )


def transform_standings(df: DataFrame) -> DataFrame:
    """
    Standings transform:
    - Parse timestamp
    - Add win_rate column
    - Deduplicate: keep latest per team + snapshot_date
    """
    return (
        df
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .withColumn(
            "win_rate",
            F.when(F.col("played") > 0, F.round(F.col("won") / F.col("played"), 3))
            .otherwise(0.0)
        )
        .withColumn(
            "row_num",
            F.row_number().over(
                Window.partitionBy("team", "snapshot_date")
                .orderBy(F.col("event_time").desc())
            )
        )
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )


# ── Read functions ───────────────────────────────────────────────

def read_from_kafka(spark: SparkSession, topic: str, schema: StructType,
                    bootstrap_servers: str) -> DataFrame:
    """Batch read từ Kafka topic — đọc toàn bộ messages hiện có"""
    raw = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )
    return (
        raw
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(F.from_json(F.col("json_str"), schema).alias("data"))
        .select("data.*")
    )


def read_from_json(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    """Read từ JSON files (backup/testing mode)"""
    return spark.read.schema(schema).json(path)


# ── Write function ───────────────────────────────────────────────

def write_parquet(df: DataFrame, output_path: str, partition_cols: list = None):
    """Ghi Parquet với partitioning"""
    writer = df.write.mode("overwrite").format("parquet")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(output_path)


# ── Main ─────────────────────────────────────────────────────────

def run(source: str, input_path: str, output_path: str, bootstrap_servers: str):
    spark = create_spark_session(output_path=output_path)

    print(f"{'='*60}")
    print(f"  EPL Spark Transformer")
    print(f"  Source: {source}")
    print(f"  Input:  {input_path or bootstrap_servers}")
    print(f"  Output: {output_path}")
    print(f"  Time:   {datetime.utcnow().isoformat()}")
    print(f"{'='*60}")

    topics = {
        "matches":   (MATCH_SCHEMA, transform_matches, ["season", "matchday"]),
        "events":    (EVENT_SCHEMA, transform_events, ["match_id"]),
        "standings": (STANDING_SCHEMA, transform_standings, ["season", "snapshot_date"]),
    }

    for name, (schema, transform_fn, partition_cols) in topics.items():
        print(f"\n--- Processing {name} ---")

        try:
            if source == "kafka":
                topic = f"epl.{name}"
                df = read_from_kafka(spark, topic, schema, bootstrap_servers)
            else:
                path = os.path.join(input_path, name)
                df = read_from_json(spark, path, schema)

            count_raw = df.count()
        except Exception as e:
            print(f"  Skipping {name} — topic not available: {e}")
            continue

        print(f"  Raw records: {count_raw}")

        if count_raw == 0:
            print(f"  Skipping {name} — no data")
            continue

        df_transformed = transform_fn(df)
        count_out = df_transformed.count()
        print(f"  After transform: {count_out}")

        out_path = os.path.join(output_path, name)
        write_parquet(df_transformed, out_path, partition_cols)
        print(f"  Written to: {out_path}")

    spark.stop()
    print(f"\n{'='*60}")
    print("  Done!")
    print(f"{'='*60}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EPL Spark Transformer")
    parser.add_argument("--source", choices=["kafka", "file"], default="kafka")
    parser.add_argument("--input", default=None, help="Input path for file mode")
    parser.add_argument("--output", default="/data/processed", help="Output parquet path")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
    )
    args = parser.parse_args()

    run(
        source=args.source,
        input_path=args.input,
        output_path=args.output,
        bootstrap_servers=args.bootstrap_servers,
    )
