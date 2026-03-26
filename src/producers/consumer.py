import json
import sys
from kafka import KafkaConsumer

# Nhận tên consumer từ command line
consumer_name = sys.argv[1] if len(sys.argv) > 1 else "consumer-1"

consumer = KafkaConsumer(
    "epl.matches",
    bootstrap_servers="localhost:9092",
    group_id="spark-group",
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
)

print(f"👂 [{consumer_name}] đang lắng nghe — assigned partitions: {consumer.assignment()}\n")

for message in consumer:
    event = message.value
    print(
        f"[{consumer_name}] partition={message.partition} offset={message.offset} | "
        f"{event['home_team']} {event['home_score']}-{event['away_score']} {event['away_team']}"
    )