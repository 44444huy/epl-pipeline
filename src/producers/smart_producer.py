import time
import logging
import os
import random
from datetime import datetime, timezone
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

from utils.kafka_utils import create_producer_with_retry, safe_send_validated
from utils.football_api import FootballAPIClient
from utils.api_mapper import (
    map_fixture_to_match,
    map_event_to_match_event,
    map_standing_to_model,
)
from producers.robust_producer import (
    generate_matches,
    simulate_tick,
    TEAMS,
)
from models.epl_models import Standing

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

POLL_INTERVAL  = 30
MOCK_INTERVAL  = 5
STANDING_EVERY = 10

def now_iso():
    return datetime.now(timezone.utc).isoformat()

# ── Mock standings fallback ──────────────────────────────────────
def publish_mock_standings(producer, dlq):
    for rank, team in enumerate(random.sample(TEAMS, len(TEAMS)), 1):
        standing = Standing(
            team=team, rank=rank, played=29,
            won=random.randint(5, 20),
            drawn=random.randint(2, 8),
            lost=random.randint(1, 10),
            goals_for=random.randint(20, 60),
            goals_against=random.randint(15, 50),
            goal_diff=random.randint(-10, 30),
            points=random.randint(15, 60),
            season="2024/25",
            timestamp=now_iso(),
        )
        safe_send_validated(
            producer, "epl.standings",
            standing.team, standing.to_json(), dlq
        )
    logger.info("📊 Mock standings published")

# ── Real data mode ───────────────────────────────────────────────
def run_real_mode(client, producer, dlq, poll_count):
    """Fetch và publish data thật từ API"""
    live_fixtures = client.get_live_matches()
    logger.info(f"📡 {len(live_fixtures)} trận EPL đang diễn ra")

    for fixture in live_fixtures:
        fixture_id = str(fixture["fixture"]["id"])

        match = map_fixture_to_match(fixture)
        if match:
            safe_send_validated(
                producer, "epl.matches",
                match.match_id, match.to_json(), dlq
            )
            logger.info(
                f"⚽ {match.home_team} {match.home_score}"
                f"-{match.away_score} {match.away_team} [{match.status}]"
            )

        try:
            events = client.get_fixture_events(int(fixture_id))
            published = 0
            for raw_event in events:
                event = map_event_to_match_event(raw_event, fixture_id)
                if event:
                    safe_send_validated(
                        producer, "epl.events",
                        event.match_id, event.to_json(), dlq
                    )
                    published += 1
            logger.info(f"   └─ {published} events published")
        except Exception as e:
            logger.error(f"❌ Fetch events thất bại: {e}")

        time.sleep(1)

    # Standings mỗi 10 polls
    if poll_count % STANDING_EVERY == 0:
        try:
            standings = client.get_standings()
            count = 0
            for raw in standings:
                standing = map_standing_to_model(raw)
                if standing:
                    safe_send_validated(
                        producer, "epl.standings",
                        standing.team, standing.to_json(), dlq
                    )
                    count += 1
            logger.info(f"📊 Real standings updated ({count} teams)")
        except Exception as e:
            logger.error(f"❌ Fetch standings thất bại: {e}")

    return len(live_fixtures)

# ── Mock data mode ───────────────────────────────────────────────
def run_mock_mode(matches, producer, dlq, tick):
    """Dùng mock data khi không có trận live"""
    live_matches = [m for m in matches if m.status == "live"]
    if not live_matches:
        return False  # hết trận

    match = random.choice(live_matches)
    event = simulate_tick(match)

    if event:
        safe_send_validated(
            producer, "epl.matches",
            match.match_id, match.to_json(), dlq
        )
        safe_send_validated(
            producer, "epl.events",
            match.match_id, event.to_json(), dlq
        )
        logger.info(
            f"[MOCK tick {tick:03d}] "
            f"{match.home_team} {match.home_score}"
            f"-{match.away_score} {match.away_team} | "
            f"{event.event_type.upper()} → {event.player}"
        )
    else:
        safe_send_validated(
            producer, "epl.matches",
            match.match_id, match.to_json(), dlq
        )
        logger.info(
            f"[MOCK tick {tick:03d}] "
            f"{match.home_team} {match.home_score}"
            f"-{match.away_score} {match.away_team} | --"
        )

    # Kết thúc trận sau 30 ticks
    if tick % 30 == 0:
        match.status = "finished"
        safe_send_validated(
            producer, "epl.matches",
            match.match_id, match.to_json(), dlq
        )
        publish_mock_standings(producer, dlq)
        logger.info(
            f"🏁 [MOCK] Trận kết thúc: "
            f"{match.home_team} {match.home_score}"
            f"-{match.away_score} {match.away_team}"
        )

    return True

# ── Main ─────────────────────────────────────────────────────────
def main():
    dlq = []
    client = FootballAPIClient()
    producer = create_producer_with_retry()

    logger.info("🏴󠁧󠁢󠁥󠁮󠁧󠁿 EPL Smart Producer bắt đầu...")

    # Kiểm tra trận tiếp theo
    try:
        next_fixture = client.get_next_fixture()
        if next_fixture:
            kickoff = next_fixture["fixture"]["date"]
            home = next_fixture["teams"]["home"]["name"]
            away = next_fixture["teams"]["away"]["name"]
            logger.info(f"📅 Trận tiếp theo: {home} vs {away} lúc {kickoff}")
    except Exception as e:
        logger.warning(f"Không lấy được next fixture: {e}")

    # Kiểm tra hôm nay có trận không
    try:
        todays = client.get_todays_fixtures()
        logger.info(f"📅 Hôm nay có {len(todays)} trận EPL")
        for f in todays:
            h = f["teams"]["home"]["name"]
            a = f["teams"]["away"]["name"]
            t = f["fixture"]["date"]
            s = f["fixture"]["status"]["short"]
            logger.info(f"   {h} vs {a} [{s}] lúc {t}")
    except Exception as e:
        logger.warning(f"Không lấy được today fixtures: {e}")

    # Chuẩn bị mock data sẵn
    mock_matches = generate_matches(matchday=29)
    mock_tick = 0
    poll_count = 0
    consecutive_failures = 0
    MAX_FAILURES = 3

    try:
        while True:
            poll_count += 1
            logger.info(f"\n─── Poll #{poll_count} ───────────────────────")

            # Thử real mode trước
            try:
                live_count = run_real_mode(client, producer, dlq, poll_count)
                consecutive_failures = 0

                if live_count == 0:
                    # Không có trận live → dùng mock trong lúc chờ
                    logger.info("💤 Không có trận live — chạy mock data...")
                    for _ in range(6):  # mock 6 ticks trong 30s
                        mock_tick += 1
                        still_live = run_mock_mode(
                            mock_matches, producer, dlq, mock_tick
                        )
                        if not still_live:
                            mock_matches = generate_matches(matchday=29)
                            mock_tick = 0
                        producer.flush()
                        time.sleep(MOCK_INTERVAL)
                else:
                    producer.flush()
                    time.sleep(POLL_INTERVAL)

            except Exception as e:
                consecutive_failures += 1
                logger.error(f"❌ Poll thất bại: {e}")

                if consecutive_failures >= MAX_FAILURES:
                    logger.warning("🔄 Recreating producer...")
                    try:
                        producer.close()
                    except Exception:
                        pass
                    producer = create_producer_with_retry(
                        max_retries=10, retry_delay=5
                    )
                    consecutive_failures = 0

                time.sleep(POLL_INTERVAL)

            if dlq:
                logger.warning(f"⚠️  DLQ: {len(dlq)} messages thất bại")

    except KeyboardInterrupt:
        logger.info("⛔ Smart Producer dừng.")
    finally:
        producer.flush()
        producer.close()
        if dlq:
            import json
            with open("dlq_smart.json", "w") as f:
                json.dump(dlq, f, indent=2, default=str)
            logger.warning(f"⚠️  {len(dlq)} messages lưu vào dlq_smart.json")
        logger.info("✅ Done.")

if __name__ == "__main__":
    main()