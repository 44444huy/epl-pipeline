import sys
sys.path.append("D:/EPL_PROJECT/epl-pipeline/src")

from utils.kafka_utils import validate_message
from datetime import datetime, timezone
import json

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def to_bytes(d: dict) -> bytes:
    return json.dumps(d).encode("utf-8")

def test_valid_match():
    msg = to_bytes({
        "match_id": "m25_01", "home_team": "Arsenal",
        "away_team": "Chelsea", "home_score": 2, "away_score": 1,
        "status": "live", "matchday": 25, "season": "2024/25",
        "venue": "Emirates Stadium", "timestamp": now_iso(),
    })
    valid, err = validate_message("epl.matches", msg)
    assert valid, f"Should be valid: {err}"
    print("✅ test_valid_match passed")

def test_invalid_status():
    """status phải là scheduled/live/finished"""
    msg = to_bytes({
        "match_id": "m25_01", "home_team": "Arsenal",
        "away_team": "Chelsea", "home_score": 0, "away_score": 0,
        "status": "in_progress",   # ← invalid!
        "matchday": 25, "season": "2024/25",
        "venue": "Emirates Stadium", "timestamp": now_iso(),
    })
    valid, err = validate_message("epl.matches", msg)
    assert not valid, "Should be invalid"
    print(f"✅ test_invalid_status passed — caught: {err}")

def test_negative_score():
    """score không được âm"""
    msg = to_bytes({
        "match_id": "m25_01", "home_team": "Arsenal",
        "away_team": "Chelsea", "home_score": -1, "away_score": 0,
        "status": "live", "matchday": 25, "season": "2024/25",
        "venue": "Emirates Stadium", "timestamp": now_iso(),
    })
    valid, err = validate_message("epl.matches", msg)
    assert not valid, "Should be invalid"
    print(f"✅ test_negative_score passed — caught: {err}")

def test_extra_field_rejected():
    """additionalProperties: False — field lạ bị reject"""
    msg = to_bytes({
        "match_id": "m25_01", "home_team": "Arsenal",
        "away_team": "Chelsea", "home_score": 0, "away_score": 0,
        "status": "live", "matchday": 25, "season": "2024/25",
        "venue": "Emirates Stadium", "timestamp": now_iso(),
        "home_goals": 0,   # ← field lạ!
    })
    valid, err = validate_message("epl.matches", msg)
    assert not valid, "Should be invalid"
    print(f"✅ test_extra_field_rejected passed — caught: {err}")

def test_valid_event():
    from uuid import uuid4
    msg = to_bytes({
        "event_id": str(uuid4()), "match_id": "m25_01",
        "event_type": "goal", "minute": 45,
        "team": "Arsenal", "player": "Saka",
        "detail": "Normal Goal", "timestamp": now_iso(),
    })
    valid, err = validate_message("epl.events", msg)
    assert valid, f"Should be valid: {err}"
    print("✅ test_valid_event passed")

def test_invalid_event_type():
    """event_type phải nằm trong enum"""
    from uuid import uuid4
    msg = to_bytes({
        "event_id": str(uuid4()), "match_id": "m25_01",
        "event_type": "offside",   # ← không có trong enum!
        "minute": 45, "team": "Arsenal", "player": "Saka",
        "detail": None, "timestamp": now_iso(),
    })
    valid, err = validate_message("epl.events", msg)
    assert not valid, "Should be invalid"
    print(f"✅ test_invalid_event_type passed — caught: {err}")

if __name__ == "__main__":
    test_valid_match()
    test_invalid_status()
    test_negative_score()
    test_extra_field_rejected()
    test_valid_event()
    test_invalid_event_type()
    print("\n✅ Tất cả schema tests passed!")