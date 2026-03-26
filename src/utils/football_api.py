import requests
import time
import logging
from dotenv import load_dotenv
import os

load_dotenv()
logger = logging.getLogger(__name__)

API_KEY  = os.getenv("API_FOOTBALL_KEY")
BASE_URL = os.getenv("API_FOOTBALL_BASE_URL", "https://v3.football.api-sports.io")
LEAGUE_ID = int(os.getenv("EPL_LEAGUE_ID", 39))
SEASON    = int(os.getenv("EPL_SEASON", 2024))


HEADERS = {
    "x-apisports-key": API_KEY,
}

class FootballAPIClient:

    def __init__(self):
        if not API_KEY:
            raise ValueError("API_FOOTBALL_KEY chưa được set trong .env")
        self.request_count = 0

    def _get(self, endpoint: str, params: dict = None) -> dict:
        """Base GET request với logging và rate limit tracking"""
        url = f"{BASE_URL}/{endpoint}"
        try:
            response = requests.get(url, headers=HEADERS, params=params, timeout=10)
            self.request_count += 1
            logger.info(f"API call #{self.request_count}: GET /{endpoint} → {response.status_code}")

            if response.status_code == 429:
                logger.warning("⚠️  Rate limit hit! Chờ 60 giây...")
                time.sleep(60)
                return self._get(endpoint, params)  # retry

            response.raise_for_status()
            return response.json()

        except requests.exceptions.Timeout:
            logger.error(f"❌ Timeout: GET /{endpoint}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Request failed: {e}")
            raise

    def get_live_matches(self) -> list:
        """Lấy tất cả trận EPL đang diễn ra"""
        data = self._get("fixtures", params={
            "league": LEAGUE_ID,
            "live": "all",
        })
        return data.get("response", [])

    def get_fixtures_by_matchday(self, matchday: int) -> list:
        """Lấy tất cả trận của 1 vòng đấu"""
        data = self._get("fixtures", params={
            "league": LEAGUE_ID,
            "season": SEASON,
            "round": f"Regular Season - {matchday}",
        })
        return data.get("response", [])

    def get_standings(self) -> list:
        """Lấy bảng xếp hạng EPL hiện tại"""
        data = self._get("standings", params={
            "league": LEAGUE_ID,
            "season": SEASON,
        })
        try:
            return data["response"][0]["league"]["standings"][0]
        except (KeyError, IndexError):
            return []

    def get_fixture_events(self, fixture_id: int) -> list:
        """Lấy tất cả events của 1 trận (goals, cards, subs)"""
        data = self._get("fixtures/events", params={
            "fixture": fixture_id,
        })
        return data.get("response", [])

    def get_remaining_requests(self) -> int:
        """Kiểm tra còn bao nhiêu request trong ngày"""
        data = self._get("status")
        try:
            return data["response"]["requests"]["limit_day"] - \
                   data["response"]["requests"]["current"]
        except (KeyError, TypeError):
            return -1
    
    def get_todays_fixtures(self) -> list:
        """Lấy tất cả trận EPL hôm nay (live + scheduled + finished)"""
        from datetime import date
        today = date.today().isoformat()
        data = self._get("fixtures", params={
            "league": LEAGUE_ID,
            "season": SEASON,
            "date": today,
        })
        return data.get("response", [])

    def get_next_fixture(self) -> dict | None:
        """
        Lấy trận tiếp theo.
        Requires paid plan — free plan sẽ trả về None thay vì crash.
        """
        try:
            data = self._get("fixtures", params={
                "league": LEAGUE_ID,
                "season": SEASON,
                "next": 1,
            })
            # Free plan trả về errors dict
            if data.get("errors"):
                logger.debug("get_next_fixture: requires paid plan, skipping.")
                return None
            fixtures = data.get("response", [])
            return fixtures[0] if fixtures else None
        except Exception as e:
            logger.debug(f"get_next_fixture failed: {e}")
            return None