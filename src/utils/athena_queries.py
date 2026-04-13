"""
Athena Query Manager — Day 23

Reusable Athena query helper:
  - Execute query + wait for result
  - Parse results to list of dicts
  - Track bytes scanned (cost awareness)
  - Pre-built analytics queries for EPL data
  - Data quality checks

Cost: Athena charges $5/TB scanned.
  → Always use partition filters (season, matchday) to reduce cost.
  → Use LIMIT for exploration queries.

Usage:
  from utils.athena_queries import AthenaQueryManager
  aq = AthenaQueryManager(database="epl_db", output_location="s3://bucket/athena-results/")
  results = aq.execute("SELECT * FROM matches LIMIT 10")
  print(results["rows"])
  print(f"Scanned: {results['bytes_scanned_mb']:.2f} MB")
"""

import os
import time
import logging
from typing import Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class AthenaQueryManager:
    """Execute Athena queries with result parsing and cost tracking."""

    def __init__(
        self,
        database: str = "epl_db",
        output_location: str = None,
        region: str = None,
        timeout_seconds: int = 60,
    ):
        self.database = database
        self.region = region or os.environ.get("AWS_REGION", "ap-southeast-1")
        self.output_location = output_location or os.environ.get(
            "ATHENA_OUTPUT", "s3://epl-pipeline-processed-nqh/athena-results/"
        )
        self.timeout_seconds = timeout_seconds
        self.client = boto3.client("athena", region_name=self.region)

        # Track cumulative cost across queries in this session
        self.total_bytes_scanned = 0
        self.query_count = 0

        logger.info(
            f"AthenaQueryManager: db={database}, region={self.region}, "
            f"output={self.output_location}"
        )

    # ── Core: Execute + Wait + Parse ───────────────────────────────

    def execute(self, query: str, parse_rows: bool = True) -> dict:
        """
        Execute an Athena query, wait for completion, return results.

        Returns:
            {
                "query_id": str,
                "state": "SUCCEEDED" | "FAILED" | "CANCELLED",
                "rows": [{"col1": "val1", ...}, ...],  # parsed rows
                "raw_rows": [[str, ...], ...],           # raw row data
                "column_names": [str, ...],
                "row_count": int,
                "bytes_scanned": int,
                "bytes_scanned_mb": float,
                "execution_time_ms": int,
                "query": str,
            }
        """
        logger.info(f"Executing Athena query: {query[:120]}...")

        # Start query
        try:
            response = self.client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={"Database": self.database},
                ResultConfiguration={"OutputLocation": self.output_location},
            )
        except ClientError as e:
            logger.error(f"Failed to start query: {e}")
            raise

        query_id = response["QueryExecutionId"]
        logger.info(f"Query started: {query_id}")

        # Wait for completion
        state = self._wait_for_query(query_id)

        # Get execution stats
        exec_info = self.client.get_query_execution(QueryExecutionId=query_id)
        stats = exec_info["QueryExecution"]["Statistics"]
        bytes_scanned = stats.get("DataScannedInBytes", 0)
        exec_time_ms = stats.get("EngineExecutionTimeInMillis", 0)

        # Track cumulative cost
        self.total_bytes_scanned += bytes_scanned
        self.query_count += 1

        result = {
            "query_id": query_id,
            "state": state,
            "query": query,
            "bytes_scanned": bytes_scanned,
            "bytes_scanned_mb": bytes_scanned / (1024 * 1024),
            "execution_time_ms": exec_time_ms,
            "rows": [],
            "raw_rows": [],
            "column_names": [],
            "row_count": 0,
        }

        if state != "SUCCEEDED":
            reason = exec_info["QueryExecution"]["Status"].get(
                "StateChangeReason", "unknown"
            )
            logger.error(f"Query {state}: {reason}")
            result["error"] = reason
            return result

        # Parse results
        if parse_rows:
            self._parse_results(query_id, result)

        logger.info(
            f"Query done: {result['row_count']} rows, "
            f"{result['bytes_scanned_mb']:.2f} MB scanned, "
            f"{exec_time_ms}ms"
        )
        return result

    def _wait_for_query(self, query_id: str) -> str:
        """Poll until query completes. Returns final state."""
        poll_interval = 1  # start 1s, increase gradually
        elapsed = 0

        while elapsed < self.timeout_seconds:
            response = self.client.get_query_execution(QueryExecutionId=query_id)
            state = response["QueryExecution"]["Status"]["State"]

            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                return state

            time.sleep(poll_interval)
            elapsed += poll_interval
            poll_interval = min(poll_interval * 1.5, 5)  # backoff, max 5s

        raise TimeoutError(
            f"Athena query {query_id} timed out after {self.timeout_seconds}s"
        )

    def _parse_results(self, query_id: str, result: dict):
        """Fetch and parse query results into list of dicts."""
        rows = []
        column_names = []
        raw_rows = []

        paginator = self.client.get_paginator("get_query_results")
        for page in paginator.paginate(QueryExecutionId=query_id):
            result_set = page["ResultSet"]

            # First page: extract column names from header row
            if not column_names:
                column_info = result_set.get("ResultSetMetadata", {}).get(
                    "ColumnInfo", []
                )
                column_names = [col["Name"] for col in column_info]
                result["column_names"] = column_names

                # First row is header in Athena results, skip it
                data_rows = result_set["Rows"][1:]
            else:
                data_rows = result_set["Rows"]

            for row in data_rows:
                values = [
                    col.get("VarCharValue", None) for col in row["Data"]
                ]
                raw_rows.append(values)
                rows.append(dict(zip(column_names, values)))

        result["rows"] = rows
        result["raw_rows"] = raw_rows
        result["row_count"] = len(rows)

    # ── Cost Tracking ──────────────────────────────────────────────

    def get_cost_summary(self) -> dict:
        """Return cumulative cost summary for this session."""
        total_mb = self.total_bytes_scanned / (1024 * 1024)
        total_tb = self.total_bytes_scanned / (1024**4)
        # Athena: $5 per TB scanned, minimum 10MB per query
        estimated_cost = max(self.query_count * 10 / (1024 * 1024), total_tb) * 5

        return {
            "query_count": self.query_count,
            "total_bytes_scanned": self.total_bytes_scanned,
            "total_mb_scanned": round(total_mb, 2),
            "estimated_cost_usd": round(estimated_cost, 6),
        }

    # ── Pre-built Analytics Queries ────────────────────────────────

    def count_matches(self, season: str = None) -> dict:
        """Count total matches, optionally filtered by season."""
        where = f"WHERE season = '{season}'" if season else ""
        return self.execute(f"SELECT COUNT(*) as total FROM matches {where}")

    def count_standings(self, season: str = None) -> dict:
        """Count standings records."""
        where = f"WHERE season = '{season}'" if season else ""
        return self.execute(f"SELECT COUNT(*) as total FROM standings {where}")

    def get_league_table(self, season: str = "2024/25") -> dict:
        """Get latest league standings sorted by rank."""
        query = f"""
        SELECT team, rank, played, won, drawn, lost,
               goals_for, goals_against, goal_diff, points, win_rate
        FROM standings
        WHERE season = '{season}'
        ORDER BY rank ASC
        """
        return self.execute(query)

    def get_recent_results(self, limit: int = 20, season: str = "2024/25") -> dict:
        """Get most recent match results."""
        query = f"""
        SELECT matchday, home_team, away_team,
               home_score, away_score, result, venue
        FROM matches
        WHERE season = '{season}'
        ORDER BY matchday DESC, event_time DESC
        LIMIT {limit}
        """
        return self.execute(query)

    def get_top_scoring_teams(self, season: str = "2024/25", limit: int = 10) -> dict:
        """Teams with most goals scored (from standings)."""
        query = f"""
        SELECT team, goals_for, goals_against, goal_diff, points
        FROM standings
        WHERE season = '{season}'
        ORDER BY goals_for DESC
        LIMIT {limit}
        """
        return self.execute(query)

    def get_high_scoring_matches(
        self, min_goals: int = 4, season: str = "2024/25"
    ) -> dict:
        """Matches with total goals >= min_goals."""
        query = f"""
        SELECT matchday, home_team, away_team,
               home_score, away_score, total_goals, venue
        FROM matches
        WHERE season = '{season}' AND total_goals >= {min_goals}
        ORDER BY total_goals DESC
        """
        return self.execute(query)

    def get_home_vs_away_stats(self, season: str = "2024/25") -> dict:
        """Home win vs away win vs draw breakdown."""
        query = f"""
        SELECT result,
               COUNT(*) as match_count,
               ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage
        FROM matches
        WHERE season = '{season}'
        GROUP BY result
        ORDER BY match_count DESC
        """
        return self.execute(query)

    def get_team_form(self, team: str, season: str = "2024/25") -> dict:
        """Get recent form for a specific team."""
        query = f"""
        SELECT matchday,
               CASE WHEN home_team = '{team}' THEN away_team ELSE home_team END as opponent,
               CASE WHEN home_team = '{team}' THEN 'Home' ELSE 'Away' END as venue,
               home_score, away_score, result
        FROM matches
        WHERE season = '{season}'
          AND (home_team = '{team}' OR away_team = '{team}')
        ORDER BY matchday DESC
        """
        return self.execute(query)

    # ── Silver Layer Views (dedup + clean) ───────────────────────

    def create_views(self) -> dict:
        """Create Silver layer views with dedup logic.

        Bronze (raw tables): matches, standings — may contain duplicates
        Silver (views): v_matches, v_standings — deduped, query-ready
        """
        results = {}

        # v_matches: dedup by match_id, keep latest record
        r = self.execute("""
        CREATE OR REPLACE VIEW v_matches AS
        SELECT match_id, home_team, away_team, home_score, away_score,
               status, venue, timestamp, event_time,
               total_goals, result, season, matchday
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY match_id ORDER BY event_time DESC
            ) as rn
            FROM matches
        )
        WHERE rn = 1
        """, parse_rows=False)
        results["v_matches"] = r["state"]
        logger.info(f"Created view v_matches: {r['state']}")

        # v_standings: dedup by team + snapshot_date, keep latest
        r = self.execute("""
        CREATE OR REPLACE VIEW v_standings AS
        SELECT team, rank, played, won, drawn, lost,
               goals_for, goals_against, goal_diff, points,
               timestamp, event_time, win_rate, season, snapshot_date
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY team, snapshot_date ORDER BY event_time DESC
            ) as rn
            FROM standings
        )
        WHERE rn = 1
        """, parse_rows=False)
        results["v_standings"] = r["state"]
        logger.info(f"Created view v_standings: {r['state']}")

        return results

    # ── Data Quality Checks ────────────────────────────────────────

    def check_row_counts(self, min_matches: int = 1, min_standings: int = 1) -> dict:
        """Verify minimum row counts exist."""
        checks = {}

        r = self.count_matches()
        match_count = int(r["rows"][0]["total"]) if r["rows"] else 0
        checks["matches"] = {
            "count": match_count,
            "passed": match_count >= min_matches,
            "threshold": min_matches,
        }

        r = self.count_standings()
        standing_count = int(r["rows"][0]["total"]) if r["rows"] else 0
        checks["standings"] = {
            "count": standing_count,
            "passed": standing_count >= min_standings,
            "threshold": min_standings,
        }

        checks["all_passed"] = all(c["passed"] for c in checks.values() if isinstance(c, dict))
        return checks

    def check_duplicates(self) -> dict:
        """Check for duplicate match_id in bronze table + verify view dedup works."""
        # Check bronze (raw) table
        query_bronze = """
        SELECT match_id, COUNT(*) as cnt
        FROM matches
        GROUP BY match_id
        HAVING COUNT(*) > 1
        """
        result_bronze = self.execute(query_bronze)
        bronze_dupes = result_bronze["row_count"]

        if bronze_dupes > 0:
            for row in result_bronze["rows"][:5]:
                logger.warning(f"  Bronze duplicate: {row['match_id']} ({row['cnt']} copies)")

        # Check silver (deduped) view
        query_silver = """
        SELECT match_id, COUNT(*) as cnt
        FROM v_matches
        GROUP BY match_id
        HAVING COUNT(*) > 1
        """
        try:
            result_silver = self.execute(query_silver)
            silver_dupes = result_silver["row_count"]
        except Exception:
            # View may not exist yet
            silver_dupes = -1
            logger.warning("  v_matches view not found, skipping silver check")

        return {
            "bronze_duplicates": bronze_dupes,
            "silver_duplicates": silver_dupes,
            "passed": silver_dupes == 0,  # Silver layer must be clean
            "details": result_bronze["rows"][:10] if bronze_dupes > 0 else [],
        }

    def check_freshness(self, max_age_days: int = 7) -> dict:
        """Check if data is fresh (most recent event_time within max_age_days)."""
        query = """
        SELECT MAX(event_time) as latest_event,
               date_diff('day', MAX(event_time), current_timestamp) as age_days
        FROM matches
        """
        result = self.execute(query)

        if not result["rows"] or result["rows"][0]["latest_event"] is None:
            return {"passed": False, "error": "No data found", "age_days": None}

        age_days = int(result["rows"][0]["age_days"])
        return {
            "latest_event": result["rows"][0]["latest_event"],
            "age_days": age_days,
            "max_age_days": max_age_days,
            "passed": age_days <= max_age_days,
        }

    def check_schema(self) -> dict:
        """Verify expected columns exist in matches table."""
        query = "SELECT * FROM matches LIMIT 0"
        result = self.execute(query)

        expected_cols = {
            "match_id", "home_team", "away_team", "home_score", "away_score",
            "status", "venue", "timestamp", "event_time", "total_goals",
            "result", "season", "matchday",
        }
        actual_cols = set(result["column_names"])
        missing = expected_cols - actual_cols

        return {
            "expected_columns": sorted(expected_cols),
            "actual_columns": sorted(actual_cols),
            "missing_columns": sorted(missing),
            "passed": len(missing) == 0,
        }

    def run_all_checks(self, min_matches: int = 1, max_age_days: int = 7) -> dict:
        """Run all data quality checks. Returns summary."""
        logger.info("Running data quality checks...")

        checks = {
            "row_counts": self.check_row_counts(min_matches=min_matches),
            "duplicates": self.check_duplicates(),
            "freshness": self.check_freshness(max_age_days=max_age_days),
            "schema": self.check_schema(),
        }

        all_passed = all(
            check.get("passed", False) or check.get("all_passed", False)
            for check in checks.values()
        )
        checks["all_passed"] = all_passed
        checks["cost"] = self.get_cost_summary()

        for name, check in checks.items():
            if name in ("all_passed", "cost"):
                continue
            status = "PASS" if check.get("passed", check.get("all_passed")) else "FAIL"
            logger.info(f"  {name}: {status}")

        logger.info(
            f"Data quality: {'ALL PASSED' if all_passed else 'SOME FAILED'} "
            f"({checks['cost']['total_mb_scanned']:.2f} MB scanned)"
        )
        return checks