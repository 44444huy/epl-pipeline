"""
EPL Events Backfill DAG — One-time historical events load

Backfill match events (goals, cards, subs) for a configurable matchday range.
Trigger manually with Airflow "Trigger DAG w/ config":
    {"matchday_start": 1, "matchday_end": 10}

Split into small ranges (~10 matchdays = ~100 API requests) to stay within the
Football-API free plan daily quota (100/day).

Prerequisite: fixtures already backfilled into `epl.matches` (needed so Spark can
join events with matches to derive season/matchday partitions).
"""

import logging
import time
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


def backfill_events_range(**context):
    """Fetch events for fixtures in the given matchday range → Kafka."""
    from utils.football_api import FootballAPIClient
    from utils.api_mapper import map_event_to_match_event
    from utils.kafka_utils import create_producer_with_retry, safe_send_validated

    conf = context["dag_run"].conf or {}
    md_start = int(conf.get("matchday_start", 1))
    md_end = int(conf.get("matchday_end", 5))
    logger.info(f"Backfilling events for matchdays {md_start}..{md_end}")

    client = FootballAPIClient()
    producer = create_producer_with_retry(bootstrap_servers=KAFKA_BOOTSTRAP)
    dlq = []
    stats = {"fixtures": 0, "events": 0, "errors": 0}

    # Check quota upfront
    remaining = client.get_remaining_requests()
    logger.info(f"API quota remaining: {remaining}")
    estimated = (md_end - md_start + 1) * 11  # 10 matches + 1 fixtures call per matchday
    if 0 < remaining < estimated:
        raise Exception(
            f"Insufficient API quota: {remaining} remaining, need ~{estimated}"
        )

    try:
        for md in range(md_start, md_end + 1):
            logger.info(f"--- Matchday {md} events ---")

            try:
                fixtures = client.get_fixtures_by_matchday(md)
            except Exception as e:
                logger.error(f"Failed to fetch fixtures md={md}: {e}")
                stats["errors"] += 1
                continue

            for fixture in fixtures:
                fixture_id = str(fixture["fixture"]["id"])
                status = fixture["fixture"]["status"]["short"]

                # Only finished matches have useful events
                if status not in ("FT", "AET", "PEN"):
                    logger.debug(f"  Skip fixture {fixture_id} (status={status})")
                    continue

                stats["fixtures"] += 1
                try:
                    events = client.get_fixture_events(int(fixture_id))
                except Exception as e:
                    logger.error(f"  Events fetch failed fixture={fixture_id}: {e}")
                    stats["errors"] += 1
                    continue

                for event in events:
                    match_event = map_event_to_match_event(event, fixture_id)
                    if not match_event:
                        stats["errors"] += 1
                        continue
                    safe_send_validated(
                        producer, "epl.events",
                        key=match_event.match_id,
                        value=match_event.to_json(),
                        dlq_messages=dlq,
                    )
                    stats["events"] += 1

            producer.flush()
            time.sleep(1)

    finally:
        producer.flush()
        producer.close()

    logger.info(f"Stats: {stats}")
    if dlq:
        logger.warning(f"DLQ: {len(dlq)} messages")
    context["ti"].xcom_push(key="events_backfill_stats", value=str(stats))


with DAG(
    dag_id="epl_backfill_events",
    default_args=default_args,
    description="Backfill events for a matchday range (manual config)",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["epl", "backfill", "events", "one-time"],
    params={
        "matchday_start": 1,
        "matchday_end": 5,
    },
) as dag:
    PythonOperator(
        task_id="backfill_events_range",
        python_callable=backfill_events_range,
    )
