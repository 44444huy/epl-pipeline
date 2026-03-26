import time
import logging
import os
from sys import path
path.append("D:/EPL_PROJECT/epl-pipeline/src")

from dotenv import load_dotenv
load_dotenv()

from utils.kafka_utils import create_producer_with_retry, safe_send_validated
from utils.football_api import FootballAPIClient
from utils.api_mapper import map_fixture_to_match, map_event_to_match_event, map_standing_to_model

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

POLL_INTERVAL   = 30   # fetch data mỗi 30 giây
STANDING_EVERY  = 10   # update standings mỗi 10 polls (~5 phút)

def main():
    dlq = []
    client = FootballAPIClient()
    producer = create_producer_with_retry()

    logger.info("🏴󠁧󠁢󠁥󠁮󠁧󠁿 EPL Real Producer bắt đầu...")

    # Kiểm tra quota trước
    remaining = client.get_remaining_requests()
    logger.info(f"📊 API requests còn lại hôm nay: {remaining}")
    if remaining < 10:
        logger.warning("⚠️  Quota thấp! Cẩn thận dùng API.")

    poll_count = 0
    consecutive_failures = 0
    MAX_FAILURES = 3

    try:
        while True:
            poll_count += 1
            logger.info(f"\n─── Poll #{poll_count} ───────────────────────")

            # ── Fetch live matches ──────────────────────────
            try:
                live_fixtures = client.get_live_matches()
                logger.info(f"📡 {len(live_fixtures)} trận EPL đang diễn ra")
            except Exception as e:
                logger.error(f"❌ Fetch live matches thất bại: {e}")
                consecutive_failures += 1
                time.sleep(POLL_INTERVAL)
                continue

            # ── Process từng trận ──────────────────────────
            for fixture in live_fixtures:
                fixture_id = str(fixture["fixture"]["id"])

                # Map + validate + publish match
                match = map_fixture_to_match(fixture)
                if match:
                    safe_send_validated(
                        producer, "epl.matches",
                        match.match_id, match.to_json(), dlq
                    )
                    logger.info(
                        f"⚽ {match.home_team} {match.home_score}"
                        f"-{match.away_score} {match.away_team} "
                        f"[{match.status}]"
                    )

                # Fetch + publish events
                try:
                    events = client.get_fixture_events(int(fixture_id))
                    for raw_event in events:
                        event = map_event_to_match_event(raw_event, fixture_id)
                        if event:
                            safe_send_validated(
                                producer, "epl.events",
                                event.match_id, event.to_json(), dlq
                            )
                    logger.info(f"   └─ {len(events)} events published")
                except Exception as e:
                    logger.error(f"❌ Fetch events thất bại fixture={fixture_id}: {e}")

                # Tránh spam API
                time.sleep(1)

            # ── Update standings mỗi 10 polls ──────────────
            if poll_count % STANDING_EVERY == 0:
                try:
                    standings = client.get_standings()
                    for raw_standing in standings:
                        standing = map_standing_to_model(raw_standing)
                        if standing:
                            safe_send_validated(
                                producer, "epl.standings",
                                standing.team, standing.to_json(), dlq
                            )
                    logger.info(f"📊 Standings updated ({len(standings)} teams)")
                except Exception as e:
                    logger.error(f"❌ Fetch standings thất bại: {e}")

            # ── Flush + DLQ report ──────────────────────────
            producer.flush()
            if dlq:
                logger.warning(f"⚠️  DLQ: {len(dlq)} message thất bại")

            consecutive_failures = 0
            logger.info(f"⏳ Chờ {POLL_INTERVAL}s đến poll tiếp theo...")
            time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        logger.info("⛔ Real Producer dừng.")
    finally:
        producer.flush()
        producer.close()
        if dlq:
            import json
            with open("dlq_real.json", "w") as f:
                json.dump(dlq, f, indent=2, default=str)
            logger.warning(f"⚠️  {len(dlq)} messages lưu vào dlq_real.json")
        logger.info("✅ Done.")

if __name__ == "__main__":
    main()