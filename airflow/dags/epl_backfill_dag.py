"""
EPL Backfill DAG — One-time historical data load

Fetch real EPL 2024/25 season data from API → push to Kafka.
Trigger manually, NOT scheduled. Run once before daily pipeline.

API free plan: 100 requests/day.
  - fixtures only (no events): ~40 requests for full season
  - with events: ~400 requests → need multiple days

Strategy:
  - Split into groups of matchdays to stay within daily API quota
  - Each task group is independent → can retry individually
  - Standings fetched once at the end
"""

import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "kafka:29092"

default_args = {
    "owner": "epl-pipeline",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="epl_backfill_season",
    default_args=default_args,
    description="One-time backfill: EPL 2024/25 real data → Kafka",
    schedule_interval=None,  # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["epl", "backfill", "one-time"],
) as dag:

    def backfill_matchdays(matchday_start, matchday_end, fetch_events=False, **context):
        """Fetch fixtures for a range of matchdays and push to Kafka."""
        from utils.football_api import FootballAPIClient
        from utils.api_mapper import map_fixture_to_match, map_event_to_match_event
        from utils.kafka_utils import create_producer_with_retry, safe_send_validated

        client = FootballAPIClient()
        producer = create_producer_with_retry(bootstrap_servers=KAFKA_BOOTSTRAP)
        dlq = []
        stats = {"matches": 0, "events": 0, "errors": 0}

        try:
            for md in range(matchday_start, matchday_end + 1):
                logger.info(f"--- Matchday {md} ---")

                try:
                    fixtures = client.get_fixtures_by_matchday(md)
                except Exception as e:
                    logger.error(f"Failed matchday {md}: {e}")
                    stats["errors"] += 1
                    continue

                for fixture in fixtures:
                    match = map_fixture_to_match(fixture)
                    if not match:
                        stats["errors"] += 1
                        continue

                    safe_send_validated(
                        producer, "epl.matches",
                        key=match.match_id,
                        value=match.to_json(),
                        dlq_messages=dlq,
                    )
                    stats["matches"] += 1
                    logger.info(f"  {match.home_team} {match.home_score}-{match.away_score} {match.away_team}")

                    # Fetch events if enabled
                    if fetch_events and match.status == "finished":
                        fixture_id = str(fixture["fixture"]["id"])
                        try:
                            events = client.get_fixture_events(int(fixture_id))
                            for event in events:
                                match_event = map_event_to_match_event(event, fixture_id)
                                if match_event:
                                    safe_send_validated(
                                        producer, "epl.events",
                                        key=match_event.match_id,
                                        value=match_event.to_json(),
                                        dlq_messages=dlq,
                                    )
                                    stats["events"] += 1
                        except Exception as e:
                            logger.error(f"  Events failed for {fixture_id}: {e}")
                            stats["errors"] += 1

                producer.flush()
                import time
                time.sleep(1)  # rate limit between matchdays

        finally:
            producer.flush()
            producer.close()

        logger.info(f"Stats: {stats}")
        if dlq:
            logger.warning(f"DLQ: {len(dlq)} messages")
        context["ti"].xcom_push(key="backfill_stats", value=str(stats))

    def backfill_standings(**context):
        """Fetch current standings and push to Kafka."""
        from utils.football_api import FootballAPIClient
        from utils.api_mapper import map_standing_to_model
        from utils.kafka_utils import create_producer_with_retry, safe_send_validated

        client = FootballAPIClient()
        producer = create_producer_with_retry(bootstrap_servers=KAFKA_BOOTSTRAP)
        dlq = []
        count = 0

        try:
            standings = client.get_standings()
            snapshot_date = datetime.utcnow().date().isoformat()

            for s in standings:
                standing = map_standing_to_model(s, snapshot_date=snapshot_date)
                if standing:
                    safe_send_validated(
                        producer, "epl.standings",
                        key=standing.team,
                        value=standing.to_json(),
                        dlq_messages=dlq,
                    )
                    count += 1
                    logger.info(f"  #{standing.rank} {standing.team} - {standing.points} pts")
        finally:
            producer.flush()
            producer.close()

        logger.info(f"Standings published: {count}")
        context["ti"].xcom_push(key="standings_count", value=count)

    def check_api_quota(**context):
        """Check API quota before starting."""
        from utils.football_api import FootballAPIClient

        client = FootballAPIClient()
        remaining = client.get_remaining_requests()
        logger.info(f"API quota remaining: {remaining}")
        context["ti"].xcom_push(key="api_remaining", value=remaining)

        if 0 < remaining < 10:
            raise Exception(f"API quota too low: {remaining} requests remaining")

    def backfill_summary(**context):
        """Log summary of all backfill tasks."""
        ti = context["ti"]
        logger.info("=" * 60)
        logger.info("  BACKFILL SUMMARY")
        logger.info("=" * 60)

        for task_id in ["backfill_md_1_10", "backfill_md_11_20",
                        "backfill_md_21_30", "backfill_md_31_38"]:
            stats = ti.xcom_pull(task_ids=task_id, key="backfill_stats") or "skipped"
            logger.info(f"  {task_id}: {stats}")

        standings = ti.xcom_pull(task_ids="backfill_standings", key="standings_count") or 0
        logger.info(f"  standings: {standings} teams")
        logger.info("=" * 60)

    # ── Tasks ──────────────────────────────────────────────────
    t_check = PythonOperator(
        task_id="check_api_quota",
        python_callable=check_api_quota,
    )

    # Split 38 matchdays into 4 groups (~10 requests each = ~40 total)
    t_md_1 = PythonOperator(
        task_id="backfill_md_1_10",
        python_callable=backfill_matchdays,
        op_kwargs={"matchday_start": 1, "matchday_end": 10},
    )

    t_md_2 = PythonOperator(
        task_id="backfill_md_11_20",
        python_callable=backfill_matchdays,
        op_kwargs={"matchday_start": 11, "matchday_end": 20},
    )

    t_md_3 = PythonOperator(
        task_id="backfill_md_21_30",
        python_callable=backfill_matchdays,
        op_kwargs={"matchday_start": 21, "matchday_end": 30},
    )

    t_md_4 = PythonOperator(
        task_id="backfill_md_31_38",
        python_callable=backfill_matchdays,
        op_kwargs={"matchday_start": 31, "matchday_end": 38},
    )

    t_standings = PythonOperator(
        task_id="backfill_standings",
        python_callable=backfill_standings,
    )

    t_summary = PythonOperator(
        task_id="backfill_summary",
        python_callable=backfill_summary,
    )

    # ── Flow: check quota → 4 groups sequential → standings → summary ──
    # Sequential vì API rate limit, ko nên gọi song song
    t_check >> t_md_1 >> t_md_2 >> t_md_3 >> t_md_4 >> t_standings >> t_summary
