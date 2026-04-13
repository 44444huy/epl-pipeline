"""
Glue Catalog Manager — Day 22

Tạo Glue Database + Tables bằng boto3 API (free tier).
Không dùng Glue Crawler (tốn tiền).

Sau khi tạo table, Athena có thể query trực tiếp:
  SELECT * FROM epl_db.matches WHERE season = '2024/25'
"""

import os
import logging
from datetime import datetime
from urllib.parse import unquote

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class GlueCatalogManager:
    """Manage Glue Data Catalog databases and tables."""

    def __init__(
        self,
        database: str = "epl_db",
        region: str = None,
    ):
        self.database = database
        self.region = region or os.environ.get("AWS_REGION", "ap-southeast-1")
        self.glue_client = boto3.client("glue", region_name=self.region)
        logger.info(f"GlueCatalogManager: database={database}, region={self.region}")

    def create_database(self) -> bool:
        """Create Glue database if not exists."""
        try:
            self.glue_client.get_database(Name=self.database)
            logger.info(f"Database '{self.database}' already exists")
            return True
        except self.glue_client.exceptions.EntityNotFoundException:
            pass

        try:
            self.glue_client.create_database(
                DatabaseInput={
                    "Name": self.database,
                    "Description": "EPL Pipeline - English Premier League data lake",
                }
            )
            logger.info(f"Created database '{self.database}'")
            return True
        except ClientError as e:
            logger.error(f"Failed to create database: {e}")
            return False

    def create_matches_table(self, s3_location: str) -> bool:
        """Create Glue table for matches Parquet data."""
        return self._create_table(
            table_name="matches",
            s3_location=f"{s3_location}/matches/",
            columns=[
                {"Name": "match_id", "Type": "string"},
                {"Name": "home_team", "Type": "string"},
                {"Name": "away_team", "Type": "string"},
                {"Name": "home_score", "Type": "int"},
                {"Name": "away_score", "Type": "int"},
                {"Name": "status", "Type": "string"},
                {"Name": "venue", "Type": "string"},
                {"Name": "timestamp", "Type": "string"},
                {"Name": "event_time", "Type": "timestamp"},
                {"Name": "total_goals", "Type": "int"},
                {"Name": "result", "Type": "string"},
            ],
            partition_keys=[
                {"Name": "season", "Type": "string"},
                {"Name": "matchday", "Type": "string"},
            ],
            description="EPL match results - enriched with total_goals, result",
        )

    def create_standings_table(self, s3_location: str) -> bool:
        """Create Glue table for standings Parquet data."""
        return self._create_table(
            table_name="standings",
            s3_location=f"{s3_location}/standings/",
            columns=[
                {"Name": "team", "Type": "string"},
                {"Name": "rank", "Type": "int"},
                {"Name": "played", "Type": "int"},
                {"Name": "won", "Type": "int"},
                {"Name": "drawn", "Type": "int"},
                {"Name": "lost", "Type": "int"},
                {"Name": "goals_for", "Type": "int"},
                {"Name": "goals_against", "Type": "int"},
                {"Name": "goal_diff", "Type": "int"},
                {"Name": "points", "Type": "int"},
                {"Name": "timestamp", "Type": "string"},
                {"Name": "event_time", "Type": "timestamp"},
                {"Name": "win_rate", "Type": "double"},
            ],
            partition_keys=[
                {"Name": "season", "Type": "string"},
                {"Name": "snapshot_date", "Type": "string"},
            ],
            description="EPL standings - enriched with win_rate",
        )

    def _create_table(
        self,
        table_name: str,
        s3_location: str,
        columns: list,
        partition_keys: list,
        description: str = "",
    ) -> bool:
        """Create or update a Glue table."""
        table_input = {
            "Name": table_name,
            "Description": description,
            "StorageDescriptor": {
                "Columns": columns,
                "Location": s3_location,
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    "Parameters": {"serialization.format": "1"},
                },
                "Compressed": True,
            },
            "PartitionKeys": partition_keys,
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {
                "classification": "parquet",
                "compressionType": "snappy",
                "parquet.compression": "SNAPPY",
                "EXTERNAL": "TRUE",
                "last_updated": datetime.utcnow().isoformat(),
            },
        }

        # Drop and recreate to handle partition key type changes
        try:
            self.glue_client.delete_table(
                DatabaseName=self.database,
                Name=table_name,
            )
            logger.info(f"Dropped existing table '{self.database}.{table_name}'")
        except self.glue_client.exceptions.EntityNotFoundException:
            pass

        try:
            self.glue_client.create_table(
                DatabaseName=self.database,
                TableInput=table_input,
            )
            logger.info(f"Created table '{self.database}.{table_name}'")
            return True
        except ClientError as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            return False

    def add_partitions(self, table_name: str, s3_location: str) -> int:
        """Auto-discover and add partitions from S3 structure."""
        s3_client = boto3.client("s3", region_name=self.region)

        # Get expected partition key count from table definition
        try:
            table_resp = self.glue_client.get_table(
                DatabaseName=self.database, Name=table_name
            )
            expected_keys = len(table_resp["Table"]["PartitionKeys"])
        except ClientError:
            expected_keys = None

        # Parse bucket and prefix from s3:// path
        path = s3_location.replace("s3://", "")
        bucket = path.split("/")[0]
        prefix = "/".join(path.split("/")[1:])
        if not prefix.endswith("/"):
            prefix += "/"

        # List all parquet files to discover partition paths
        partitions_found = set()
        paginator = s3_client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".parquet"):
                    continue

                # Extract partition path: everything between prefix and filename
                relative = key[len(prefix):]
                parts = relative.split("/")
                if len(parts) > 1:
                    partition_path = "/".join(parts[:-1])
                    partitions_found.add(partition_path)

        added = 0
        for partition_path in sorted(partitions_found):
            # Parse partition values: "season=2024%2F25/matchday=32"
            values = []
            for part in partition_path.split("/"):
                if "=" in part:
                    values.append(unquote(part.split("=", 1)[1]))

            if not values:
                continue

            # Skip Hive default partitions (null values)
            if "__HIVE_DEFAULT_PARTITION__" in partition_path:
                logger.debug(f"  Skipping default partition: {partition_path}")
                continue

            # Skip paths with wrong number of partition values
            # e.g. year=2026/month=04/day=06/season=.../matchday=... has 5 values but only 2 keys
            if expected_keys and len(values) != expected_keys:
                logger.debug(f"  Skipping partition (expected {expected_keys} keys, got {len(values)}): {partition_path}")
                continue

            partition_location = f"{s3_location.rstrip('/')}/{partition_path}/"

            try:
                self.glue_client.create_partition(
                    DatabaseName=self.database,
                    TableName=table_name,
                    PartitionInput={
                        "Values": values,
                        "StorageDescriptor": {
                            "Columns": self._get_table_columns(table_name),
                            "Location": partition_location,
                            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                            "SerdeInfo": {
                                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                            },
                        },
                    },
                )
                added += 1
                logger.info(f"  Added partition: {partition_path}")
            except self.glue_client.exceptions.AlreadyExistsException:
                logger.debug(f"  Partition exists: {partition_path}")
            except ClientError as e:
                logger.error(f"  Failed to add partition {partition_path}: {e}")

        logger.info(f"Table '{table_name}': {added} new partitions added, {len(partitions_found)} total")
        return added

    def _get_table_columns(self, table_name: str) -> list:
        """Get column definitions from existing table."""
        try:
            response = self.glue_client.get_table(
                DatabaseName=self.database,
                Name=table_name,
            )
            return response["Table"]["StorageDescriptor"]["Columns"]
        except ClientError:
            return []

    def setup_all(self, s3_base: str) -> dict:
        """Create database + all tables + add partitions. One-stop setup."""
        results = {}

        # Create database
        results["database"] = self.create_database()

        # Create tables
        results["matches_table"] = self.create_matches_table(s3_base)
        results["standings_table"] = self.create_standings_table(s3_base)

        # Add partitions
        results["matches_partitions"] = self.add_partitions(
            "matches", f"s3://{s3_base.split('//')[1]}" if s3_base.startswith("s3a://") else s3_base
        )
        results["standings_partitions"] = self.add_partitions(
            "standings", f"s3://{s3_base.split('//')[1]}" if s3_base.startswith("s3a://") else s3_base
        )

        logger.info(f"Glue Catalog setup complete: {results}")
        return results
