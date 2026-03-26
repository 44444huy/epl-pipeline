# JSON Schemas cho EPL topics
# Đây là "hợp đồng" — bất kỳ message nào không khớp sẽ bị reject

MATCH_SCHEMA = {
    "type": "object",
    "required": [
        "match_id", "home_team", "away_team",
        "home_score", "away_score", "status",
        "matchday", "season", "venue", "timestamp"
    ],
    "properties": {
        "match_id":    {"type": "string"},
        "home_team":   {"type": "string"},
        "away_team":   {"type": "string"},
        "home_score":  {"type": "integer", "minimum": 0},
        "away_score":  {"type": "integer", "minimum": 0},
        "status":      {"type": "string", "enum": ["scheduled", "live", "finished"]},
        "matchday":    {"type": "integer", "minimum": 1, "maximum": 38},
        "season":      {"type": "string", "pattern": "^[0-9]{4}/[0-9]{2}$"},
        "venue":       {"type": "string"},
        "timestamp":   {"type": "string"},
    },
    "additionalProperties": False,  # không cho phép field lạ
}

MATCH_EVENT_SCHEMA = {
    "type": "object",
    "required": [
        "event_id", "match_id", "event_type",
        "minute", "team", "player", "timestamp"
    ],
    "properties": {
        "event_id":   {"type": "string"},
        "match_id":   {"type": "string"},
        "event_type": {
            "type": "string",
            "enum": ["goal", "yellow_card", "red_card", "substitution"]
        },
        "minute":     {"type": "integer", "minimum": 1, "maximum": 120},
        "team":       {"type": "string"},
        "player":     {"type": "string"},
        "detail":     {"type": ["string", "null"]},
        "timestamp":  {"type": "string"},
    },
    "additionalProperties": False,
}

STANDING_SCHEMA = {
    "type": "object",
    "required": [
        "team", "rank", "played", "won", "drawn", "lost",
        "goals_for", "goals_against", "goal_diff", "points",
        "season", "timestamp"
    ],
    "properties": {
        "team":           {"type": "string"},
        "rank":           {"type": "integer", "minimum": 1, "maximum": 20},
        "played":         {"type": "integer", "minimum": 0},
        "won":            {"type": "integer", "minimum": 0},
        "drawn":          {"type": "integer", "minimum": 0},
        "lost":           {"type": "integer", "minimum": 0},
        "goals_for":      {"type": "integer", "minimum": 0},
        "goals_against":  {"type": "integer", "minimum": 0},
        "goal_diff":      {"type": "integer"},
        "points":         {"type": "integer", "minimum": 0},
        "season":         {"type": "string"},
        "timestamp":      {"type": "string"},
    },
    "additionalProperties": False,
}

# Map topic → schema để dùng trong validator
TOPIC_SCHEMAS = {
    "epl.matches":   MATCH_SCHEMA,
    "epl.events":    MATCH_EVENT_SCHEMA,
    "epl.standings": STANDING_SCHEMA,
}