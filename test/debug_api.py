import sys
sys.path.append("D:/EPL_PROJECT/epl-pipeline/src")
from dotenv import load_dotenv
load_dotenv()

from utils.football_api import FootballAPIClient
import json

client = FootballAPIClient()

# Xem raw status response
print("=== RAW STATUS ===")
status = client._get("status")
print(json.dumps(status, indent=2))

# Xem raw standings response
print("\n=== RAW STANDINGS (first item) ===")
data = client._get("standings", params={"league": 39, "season": 2024})
try:
    first = data["response"][0]["league"]["standings"][0][0]
    print(json.dumps(first, indent=2))
except Exception as e:
    print(f"Lỗi parse: {e}")
    print(json.dumps(data, indent=2)[:500])  # in 500 chars đầu