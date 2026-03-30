from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional
import json

# ── Match ───────────────────────────────────────────────────────
@dataclass
class Match:
    match_id:      str
    home_team:     str
    away_team:     str
    home_score:    int
    away_score:    int
    status:        str        # "scheduled" | "live" | "finished"
    matchday:      int        # vòng đấu 1-38
    season:        str        # "2024/25"
    venue:         str
    timestamp:     str        # ISO format

    def to_json(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf-8")


# ── Match Event (goal, card, substitution) ──────────────────────
@dataclass
class MatchEvent:
    event_id:      str
    match_id:      str        # FK → Match
    event_type:    str        # "goal" | "yellow_card" | "red_card" | "substitution"
    minute:        int
    team:          str
    player:        str
    detail:        Optional[str]   # "Normal Goal", "Own Goal", "Penalty"
    timestamp:     str

    def to_json(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf-8")


# ── Standing ────────────────────────────────────────────────────
@dataclass
class Standing:
    team:          str
    rank:          int
    played:        int
    won:           int
    drawn:         int
    lost:          int
    goals_for:     int
    goals_against: int
    goal_diff:     int
    points:        int
    season:        str
    timestamp:     str        # thời điểm snapshot được lấy (real-time)
    snapshot_date: str = ""        # execution_date của DAG run (context['ds'])

    def to_json(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf-8")