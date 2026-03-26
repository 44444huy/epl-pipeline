import sys
sys.path.append("D:/EPL_PROJECT/epl-pipeline/src")
from dotenv import load_dotenv
load_dotenv()

from utils.football_api import FootballAPIClient
from utils.api_mapper import map_fixture_to_match, map_standing_to_model
import json

client = FootballAPIClient()

# Test 1: Standings
print("=== STANDINGS ===")
standings = client.get_standings()
for s in standings[:5]:  # top 5
    standing = map_standing_to_model(s)
    if standing:
        print(f"  {standing.rank}. {standing.team} — {standing.points} pts")

# Test 2: Fixtures matchday 29
print("\n=== FIXTURES Matchday 29 ===")
fixtures = client.get_fixtures_by_matchday(29)
for f in fixtures[:3]:
    match = map_fixture_to_match(f)
    if match:
        print(f"  {match.home_team} vs {match.away_team} [{match.status}]")

print(f"\n📊 Requests dùng: {client.request_count}")