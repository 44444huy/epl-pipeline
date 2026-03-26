import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    key_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

MATCHES = [
    {"match_id": "m001", "home": "Arsenal",   "away": "Chelsea"},
    {"match_id": "m002", "home": "Liverpool", "away": "Man City"},
    {"match_id": "m003", "home": "Man Utd",   "away": "Tottenham"},
]

def make_match_event(match):
    return {
        "match_id":   match["match_id"],
        "home_team":  match["home"],
        "away_team":  match["away"],
        "home_score": random.randint(0, 4),
        "away_score": random.randint(0, 4),
        "minute":     random.randint(1, 90),
        "timestamp":  datetime.utcnow().isoformat(),
    }


print("🚀 Producer đang chạy... Nhấn Ctrl+C để dừng\n")


try:
    while True:
        match = random.choice(MATCHES)
        event = make_match_event(match)

        future = producer.send(
            topic="epl.matches",
            key=match["match_id"],   # key = match_id → cùng trận vào cùng partition
            value=event,
        )

        # Chờ confirm gửi thành công
        record_metadata = future.get(timeout=10)

        print(
            f"✅ Sent → partition={record_metadata.partition} "
            f"offset={record_metadata.offset} | "
            f"{event['home_team']} {event['home_score']}-{event['away_score']} {event['away_team']} "
            f"(phút {event['minute']})"
        )
        time.sleep(3)

except KeyboardInterrupt:
    print("\n⛔ Producer dừng.")

finally:
    producer.flush()
    producer.close()
    print("✅ Producer closed cleanly.")