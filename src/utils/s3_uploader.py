"""
S3 Uploader — Day 21

Upload local files (Parquet, JSON, etc.) to S3 with Data Lake structure.

S3 Layout:
  s3://{bucket}/
    raw/epl/{topic}/year=YYYY/month=MM/day=DD/          ← raw JSON from Kafka
    processed/epl/{topic}/year=YYYY/month=MM/day=DD/     ← transformed Parquet

Usage:
  from utils.s3_uploader import S3Uploader
  uploader = S3Uploader(bucket="epl-pipeline-processed-nqh")
  uploader.upload_directory("/tmp/epl-spark-output/matches", "processed/epl/matches")
"""

import os
import logging
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3Uploader:
    """Upload files to S3 with data lake partitioning."""

    def __init__(
        self,
        bucket: str,
        region: str = None,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
    ):
        self.bucket = bucket
        self.region = region or os.environ.get("AWS_REGION", "ap-southeast-1")

        session_kwargs = {"region_name": self.region}
        if aws_access_key_id and aws_secret_access_key:
            session_kwargs["aws_access_key_id"] = aws_access_key_id
            session_kwargs["aws_secret_access_key"] = aws_secret_access_key

        self.s3_client = boto3.client("s3", **session_kwargs)
        logger.info(f"S3Uploader initialized: bucket={bucket}, region={self.region}")

    def upload_file(self, local_path: str, s3_key: str) -> dict:
        """Upload a single file to S3."""
        try:
            file_size = os.path.getsize(local_path)
            self.s3_client.upload_file(local_path, self.bucket, s3_key)
            logger.info(f"  Uploaded: {s3_key} ({file_size:,} bytes)")
            return {"status": "success", "key": s3_key, "size": file_size}
        except ClientError as e:
            logger.error(f"  Failed to upload {s3_key}: {e}")
            return {"status": "error", "key": s3_key, "error": str(e)}

    def upload_directory(
        self,
        local_dir: str,
        s3_prefix: str,
        date_partition: bool = True,
    ) -> dict:
        """
        Upload all files in a directory (recursively) to S3.

        Args:
            local_dir: Local directory path
            s3_prefix: S3 key prefix (e.g., "processed/epl/matches")
            date_partition: If True, add year=YYYY/month=MM/day=DD to path
        """
        if not os.path.exists(local_dir):
            logger.warning(f"Directory not found: {local_dir}")
            return {"uploaded": 0, "failed": 0, "skipped_dir": local_dir}

        # Add date partition
        if date_partition:
            now = datetime.utcnow()
            date_prefix = f"year={now.year}/month={now.month:02d}/day={now.day:02d}"
            s3_prefix = f"{s3_prefix}/{date_prefix}"

        uploaded = 0
        failed = 0
        total_size = 0

        for root, dirs, files in os.walk(local_dir):
            for filename in files:
                # Skip Spark metadata files
                if filename.startswith("_") or filename.startswith("."):
                    continue

                local_path = os.path.join(root, filename)
                # Preserve subdirectory structure (partition folders)
                relative_path = os.path.relpath(local_path, local_dir)
                s3_key = f"{s3_prefix}/{relative_path}".replace("\\", "/")

                result = self.upload_file(local_path, s3_key)
                if result["status"] == "success":
                    uploaded += 1
                    total_size += result["size"]
                else:
                    failed += 1

        summary = {
            "uploaded": uploaded,
            "failed": failed,
            "total_size_bytes": total_size,
            "s3_prefix": f"s3://{self.bucket}/{s3_prefix}",
        }
        logger.info(f"Upload summary: {summary}")
        return summary

    def list_objects(self, prefix: str, max_keys: int = 100) -> list:
        """List objects in S3 under a prefix."""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket, Prefix=prefix, MaxKeys=max_keys
            )
            objects = response.get("Contents", [])
            return [
                {"key": obj["Key"], "size": obj["Size"], "modified": str(obj["LastModified"])}
                for obj in objects
            ]
        except ClientError as e:
            logger.error(f"Failed to list objects: {e}")
            return []

    def check_bucket_exists(self) -> bool:
        """Verify the S3 bucket exists and is accessible."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket)
            logger.info(f"Bucket {self.bucket} is accessible")
            return True
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "404":
                logger.error(f"Bucket {self.bucket} does not exist")
            elif error_code == "403":
                logger.error(f"No permission to access bucket {self.bucket}")
            else:
                logger.error(f"Bucket check failed: {e}")
            return False