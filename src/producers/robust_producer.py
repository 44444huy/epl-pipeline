import time
import uuid
import random
import json
from datetime import datetime, timezone
from sys import path
path.append("D:/EPL_PROJECT/epl-pipeline/src")

from models.epl_models import Match, MatchEvent, Standing
from utils.kafka_utils import create_producer_with_retry, safe_send_validated
import logging

logger = logging.getLogger(__name__)

# ── Data (giữ nguyên từ mock_producer.py) ───────────────────────
TEAMS = [
    "Arsenal", "Chelsea", "Liverpool", "Man City", "Man Utd",
    "Tottenham", "Newcastle", "Aston Villa", "Brighton", "West Ham"
]

VENUES = {
    "Arsenal":     "Emirates Stadium",
    "Chelsea":     "Stamford Bridge",
    "Liverpool":   "Anfield",
    "Man City":    "Etihad Stadium",
    "Man Utd":     "Old Trafford",
    "Tottenham":   "Tottenham Hotspur Stadium",
    "Newcastle":   "St. James' Park",
    "Aston Villa": "Villa Park",
    "Brighton":    "Amex Stadium",
    "West Ham":    "London Stadium",
}

PLAYERS = {
    "Arsenal":     ["Saka", "Odegaard", "Havertz", "Jesus", "White"],
    "Chelsea":     ["Palmer", "Jackson", "Mudryk", "Gallagher", "Silva"],
    "Liverpool":   ["Salah", "Nunez", "Diaz", "Mac Allister", "Alexander-Arnold"],
    "Man City":    ["Haaland", "De Bruyne", "Foden", "Rodri", "Bernardo"],
    "Man Utd":     ["Rashford", "Fernandes", "Hojlund", "Mount", "Maguire"],
    "Tottenham":   ["Son", "Kulusevski", "Maddison", "Romero", "Bissouma"],
    "Newcastle":   ["Isak", "Gordon", "Almiron", "Trippier", "Bruno"],
    "Aston Villa": ["Watkins", "Diaby", "McGinn", "Kamara", "Cash"],
    "Brighton":    ["Welbeck", "Mitoma", "Gross", "Dunk", "Steele"],
    "West Ham":    ["Bowen", "Antonio", "Ward-Prowse", "Soucek", "Areola"],
}

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def generate_matches(matchday=25):
    teams = TEAMS.copy()
    random.shuffle(teams)
    matches = []
    for i in range(0, len(teams) - 1, 2):
        home, away = teams[i], teams[i + 1]
        matches.append(Match(
            match_id=f"m{matchday}_{i // 2 + 1:02d}",
            home_team=home,
            away_team=away,
            home_score=0,
            away_score=0,
            status="live",
            matchday=matchday,
            season="2024/25",
            venue=VENUES[home],
            timestamp=now_iso(),
        ))
    return matches

def simulate_tick(match: Match):
    event_type = random.choices(
        ["goal", "yellow_card", "red_card", "substitution", "none"],
        weights=[15, 25, 3, 15, 42]
    )[0]

    if event_type == "none":
        return None

    scoring_team = random.choice([match.home_team, match.away_team])
    player = random.choice(PLAYERS[scoring_team])

    if event_type == "goal":
        if scoring_team == match.home_team:
            match.home_score += 1
        else:
            match.away_score += 1
        detail = random.choice(["Normal Goal", "Header", "Penalty", "Free Kick"])
    else:
        detail = None

    return MatchEvent(
        event_id=str(uuid.uuid4()),
        match_id=match.match_id,
        event_type=event_type,
        minute=random.randint(1, 90),
        team=scoring_team,
        player=player,
        detail=detail,
        timestamp=now_iso(),
    )

# ── Main ────────────────────────────────────────────────────────
def main():
    # Dead Letter Queue — lưu message thất bại
    dlq: list = []

    # Tạo producer với retry
    producer = create_producer_with_retry(
        bootstrap_servers="localhost:9092",
        max_retries=5,
        retry_delay=5,
    )

    matches = generate_matches(matchday=25)
    logger.info(f"📋 Matchday 25 — {len(matches)} trận")
    for m in matches:
        logger.info(f"   {m.home_team} vs {m.away_team} @ {m.venue}")

    def finish_match(match: Match):
        match.status = "finished"
        safe_send_validated(producer, "epl.matches", match.match_id, match.to_json(), dlq)

        # Publish standings
        for rank, team in enumerate(random.sample(TEAMS, len(TEAMS)), 1):
            standing = Standing(
                team=team, rank=rank, played=25,
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
            safe_send_validated(producer, "epl.standings", team, standing.to_json(), dlq)

        producer.flush()
        logger.info(
            f"🏁 Trận kết thúc: "
            f"{match.home_team} {match.home_score}-{match.away_score} {match.away_team}"
        )
        logger.info("📊 Standings updated")

    try:
        tick = 0
        while True:
            tick += 1
            live_matches = [m for m in matches if m.status == "live"]

            if not live_matches:
                logger.info("✅ Tất cả trận đã kết thúc. Matchday 25 done!")
                break

            match = random.choice(live_matches)
            event = simulate_tick(match)

            if event:
                safe_send_validated(producer, "epl.matches", match.match_id, match.to_json(), dlq)
                safe_send_validated(producer, "epl.events", match.match_id, event.to_json(), dlq)
                logger.info(
                    f"[tick {tick:03d}] "
                    f"{match.home_team} {match.home_score}-{match.away_score} {match.away_team} | "
                    f"{event.event_type.upper()} → {event.player} (phút {event.minute})"
                )
            else:
                safe_send_validated(producer, "epl.matches", match.match_id, match.to_json(), dlq)
                logger.info(
                    f"[tick {tick:03d}] "
                    f"{match.home_team} {match.home_score}-{match.away_score} {match.away_team} | --"
                )

            if tick % 20 == 0:
                finish_match(random.choice(live_matches))

            # Log DLQ nếu có message thất bại
            if dlq:
                logger.warning(f"⚠️  DLQ hiện có {len(dlq)} message thất bại")

            time.sleep(3)

    except KeyboardInterrupt:
        logger.info("⛔ Producer dừng.")
    finally:
        producer.flush()
        producer.close()

        # Báo cáo DLQ khi tắt
        if dlq:
            logger.warning(f"⚠️  {len(dlq)} message thất bại, lưu vào dlq.json")
            with open("dlq.json", "w") as f:
                json.dump(dlq, f, indent=2, default=str)
        else:
            logger.info("✅ Không có message nào thất bại.")

        logger.info("✅ Producer closed cleanly.")

if __name__ == "__main__":
    main()