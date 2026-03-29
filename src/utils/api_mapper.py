from datetime import datetime, timezone

from typing import Optional
from models.epl_models import Match, MatchEvent, Standing
import logging

logger = logging.getLogger(__name__)

def map_fixture_to_match(fixture: dict) -> Optional[Match]:
    """Map API fixture response → Match model"""
    try:
        f = fixture["fixture"]
        teams = fixture["teams"]
        goals = fixture["goals"]
        league = fixture["league"]

        # Map status
        api_status = f["status"]["short"]
        if api_status in ("NS", "TBD"):  # not started, to be determined
            status = "scheduled"
        elif api_status in ("FT", "AET", "PEN", "AWD", "WO"):  # full time, after extra time, penalty, awarded, walkover
            status = "finished"
        else:
            status = "live"

        return Match(
            match_id=str(f["id"]),
            home_team=teams["home"]["name"],
            away_team=teams["away"]["name"],
            home_score=goals["home"] or 0,
            away_score=goals["away"] or 0,
            status=status,
            matchday=int(league["round"].split(" - ")[-1]),
            season=f"{SEASON}/{str(SEASON + 1)[-2:]}",
            venue=f["venue"]["name"] or "Unknown",
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
    except (KeyError, TypeError, ValueError) as e:
        logger.error(f"❌ map_fixture_to_match failed: {e} | data: {fixture}")
        return None

SEASON = 2024

def map_event_to_match_event(
    event: dict,
    fixture_id: str,
) -> Optional[MatchEvent]:
    """Map API event response → MatchEvent model"""
    try:
        import uuid

        # Map event type
        api_type = event["type"].lower()
        api_detail = (event.get("detail") or "").lower()

        if api_type == "goal":
            if "own goal" in api_detail:
                event_type = "goal"
                detail = "Own Goal"
            elif "penalty" in api_detail:
                event_type = "goal"
                detail = "Penalty"
            else:
                event_type = "goal"
                detail = "Normal Goal"
        elif api_type == "card":
            if "yellow" in api_detail:
                event_type = "yellow_card"
            else:
                event_type = "red_card"
            detail = None
        elif api_type == "subst":
            event_type = "substitution"
            detail = None
        else:
            logger.debug(f"Bỏ qua event type: {api_type}")
            return None

        player_name = (
            event.get("player", {}).get("name") or
            event.get("assist", {}).get("name") or
            "Unknown"
        )

        return MatchEvent(
            event_id=str(uuid.uuid4()),
            match_id=fixture_id,
            event_type=event_type,
            minute=event["time"]["elapsed"] or 0,
            team=event["team"]["name"],
            player=player_name,
            detail=detail,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
    except (KeyError, TypeError) as e:
        logger.error(f"❌ map_event_to_match_event failed: {e}")
        return None


def map_standing_to_model(standing: dict) -> Optional[Standing]:
    """Map API standing response → Standing model"""
    try:
        g = standing["all"]
        return Standing(
            team=standing["team"]["name"],
            rank=standing["rank"],
            played=g["played"],
            won=g["win"],
            drawn=g["draw"],
            lost=g["lose"],
            goals_for=g["goals"]["for"],
            goals_against=g["goals"]["against"],
            goal_diff=standing["goalsDiff"],
            points=standing["points"],
            season=f"{SEASON}/{str(SEASON + 1)[-2:]}",
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
    except (KeyError, TypeError) as e:
        logger.error(f"❌ map_standing_to_model failed: {e}")
        return None