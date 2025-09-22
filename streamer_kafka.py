import csv
import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def stream_to_kafka(file_path, topic="citibike-trips", rate=50):
    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            event = {
                "ride_id": row["ride_id"],
                "bike_type": row["rideable_type"],
                "start_station_id": row["start_station_id"],
                "end_station_id": row["end_station_id"],
                "started_at": row["started_at"],
                "ended_at": row["ended_at"],
                "user_type": row["member_casual"],
            }
            producer.send(topic, event)
            print(f"Sent: {event['ride_id']}")
            time.sleep(1.0 / rate)  # control rate

if __name__ == "__main__":
    stream_to_kafka("data/202501-citibike-tripdata.csv", rate=100)
