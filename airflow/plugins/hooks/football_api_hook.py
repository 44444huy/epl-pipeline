from airflow.hooks.base import BaseHook
import requests
import logging
import time
import os

logger = logging.getLogger(__name__)

class FootballAPIHook(BaseHook):
    """
    Airflow Hook cho Football API.
    Handles authentication, rate limiting, retry logic.
    """

    conn_name_attr = "football_api_conn_id"
    default_conn_name = "football_api_default"
    conn_type = "http"
    hook_name = "Football API"

    BASE_URL = "https://v3.football.api-sports.io"

    def __init__(self):
        super().__init__()
        self.api_key = os.getenv("API_FOOTBALL_KEY")
        if not self.api_key:
            raise ValueError("API_FOOTBALL_KEY chưa được set")
        self.headers = {"x-apisports-key": self.api_key}
        self.request_count = 0

    def _get(self, endpoint: str, params: dict = None) -> dict:
        url = f"{self.BASE_URL}/{endpoint}"
        try:
            response = requests.get(
                url, headers=self.headers,
                params=params, timeout=10
            )
            self.request_count += 1

            if response.status_code == 429:
                logger.warning("⚠️  Rate limit! Waiting 60s...")
                time.sleep(60)
                return self._get(endpoint, params)

            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ API request failed: {e}")
            raise

    def get_standings(self, league_id: int = 39, season: int = 2024) -> list:
        data = self._get("standings", params={
            "league": league_id,
            "season": season,
        })
        try:
            return data["response"][0]["league"]["standings"][0]
        except (KeyError, IndexError):
            return []

    def get_fixtures_by_date(self, date: str, league_id: int = 39, season: int = 2024) -> list:
        data = self._get("fixtures", params={
            "league": league_id,
            "season": season,
            "date": date,
        })
        return data.get("response", [])

    def get_live_fixtures(self, league_id: int = 39) -> list:
        data = self._get("fixtures", params={
            "league": league_id,
            "live": "all",
        })
        return data.get("response", [])

    def get_fixture_events(self, fixture_id: int) -> list:
        data = self._get("fixtures/events", params={"fixture": fixture_id})
        return data.get("response", [])