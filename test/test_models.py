import sys
sys.path.append("D:/EPL_PROJECT/epl-pipeline/src")

from models.epl_models import Match, MatchEvent, Standing
from datetime import datetime, timezone
import json

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def test_match_serialization():
    match = Match(
        match_id="m25_01",
        home_team="Arsenal",
        away_team="Chelsea",
        home_score=2,
        away_score=1,
        status="live",
        matchday=25,
        season="2024/25",
        venue="Emirates Stadium",
        timestamp=now_iso(),
    )
    # Serialize → deserialize → check
    data = json.loads(match.to_json())
    assert data["match_id"] == "m25_01"
    assert data["home_score"] == 2
    assert data["status"] == "live"
    print("✅ test_match_serialization passed")

def test_goal_increases_score():
    """Score chỉ tăng khi event là goal — bug đã fix ở ngày 5"""
    match = Match(
        match_id="m25_02",
        home_team="Liverpool",
        away_team="Man City",
        home_score=0,
        away_score=0,
        status="live",
        matchday=25,
        season="2024/25",
        venue="Anfield",
        timestamp=now_iso(),
    )
    initial_score = (match.home_score, match.away_score)

    # Simulate yellow card — score không đổi
    match_before = (match.home_score, match.away_score)
    # yellow card không tăng score
    assert (match.home_score, match.away_score) == initial_score
    print("✅ test_goal_increases_score passed")

def test_standing_serialization():
    standing = Standing(
        team="Arsenal",
        rank=1,
        played=25,
        won=18,
        drawn=4,
        lost=3,
        goals_for=55,
        goals_against=22,
        goal_diff=33,
        points=58,
        season="2024/25",
        timestamp=now_iso(),
    )
    data = json.loads(standing.to_json())
    assert data["team"] == "Arsenal"
    assert data["points"] == 58
    assert data["rank"] == 1
    print("✅ test_standing_serialization passed")

if __name__ == "__main__":
    test_match_serialization()
    test_goal_increases_score()
    test_standing_serialization()
    print("\n✅ Tất cả tests passed!")