import time
import uuid
import random
from datetime import datetime, timezone
from kafka import KafkaProducer
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from models.epl_models import Match, MatchEvent, Standing

# ── Setup Producer ──────────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    key_serializer=lambda k: k.encode("utf-8"),
)

# ── EPL Data ────────────────────────────────────────────────────
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

# ── Helpers ─────────────────────────────────────────────────────
def now_iso():
    return datetime.now(timezone.utc).isoformat()

# ── Generate matches ────────────────────────────────────────────
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

# ── Publish helpers ─────────────────────────────────────────────
def publish_match(match: Match):
    producer.send(
        topic="epl.matches",
        key=match.match_id,
        value=match.to_json(),
    )

def publish_event_obj(match: Match, event: MatchEvent):
    producer.send(
        topic="epl.events",
        key=match.match_id,
        value=event.to_json(),
    )

def publish_standings():
    for rank, team in enumerate(random.sample(TEAMS, len(TEAMS)), 1):
        standing = Standing(
            team=team,
            rank=rank,
            played=25,
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
        producer.send(
            topic="epl.standings",
            key=team,
            value=standing.to_json(),
        )

# ── Core logic: 1 tick = 1 sự kiện ─────────────────────────────
def simulate_tick(match: Match):
    """Quyết định event xảy ra, cập nhật score nếu là goal"""

    event_type = random.choices(
        ["goal", "yellow_card", "red_card", "substitution", "none"],
        weights=[15, 25, 3, 15, 42]
    )[0]

    if event_type == "none":
        return None

    scoring_team = random.choice([match.home_team, match.away_team])
    player = random.choice(PLAYERS[scoring_team])

    # Score chỉ tăng khi event là goal
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

# ── Finish match ────────────────────────────────────────────────
def finish_match(match: Match):
    match.status = "finished"
    publish_match(match)
    publish_standings()
    producer.flush()  # standings quan trọng, flush ngay
    print(
        f"\n🏁 Trận kết thúc: "
        f"{match.home_team} {match.home_score}-{match.away_score} {match.away_team}"
    )
    print("📊 Standings updated\n")

# ── Main ────────────────────────────────────────────────────────
print("🏴󠁧󠁢󠁥󠁮󠁧󠁿 EPL Mock Producer đang chạy...\n")

matches = generate_matches(matchday=25)
print(f"📋 Matchday 25 — {len(matches)} trận:\n")
for m in matches:
    print(f"   {m.home_team} vs {m.away_team} @ {m.venue}")
print()

try:
    tick = 0
    while True:
        tick += 1

        live_matches = [m for m in matches if m.status == "live"]

        if not live_matches:
            print("✅ Tất cả trận đã kết thúc. Matchday 25 done!")
            break

        match = random.choice(live_matches)
        event = simulate_tick(match)

        if event:
            publish_match(match)
            publish_event_obj(match, event)
            print(
                f"[tick {tick:03d}] "
                f"{match.home_team} {match.home_score}-{match.away_score} {match.away_team} | "
                f"{event.event_type.upper()} → {event.player} (phút {event.minute})"
            )
        else:
            # Không có event → vẫn publish match để giữ timestamp mới nhất
            publish_match(match)
            print(
                f"[tick {tick:03d}] "
                f"{match.home_team} {match.home_score}-{match.away_score} {match.away_team} | --"
            )

        # Kết thúc 1 trận ngẫu nhiên sau mỗi 20 ticks (~1 phút)
        if tick % 20 == 0:
            finish_match(random.choice(live_matches))

        time.sleep(3)

except KeyboardInterrupt:
    print("\n⛔ Mock Producer dừng.")
finally:
    producer.flush()
    producer.close()
    print("✅ Producer closed cleanly.")


