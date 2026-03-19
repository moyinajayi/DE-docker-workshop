import json
from kafka import KafkaConsumer


def json_deserializer(data):
    return json.loads(data.decode('utf-8'))


server = 'localhost:9092'
topic_name = 'green-trips'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='green-trips-counter',
    value_deserializer=json_deserializer,
    consumer_timeout_ms=10000  # Stop after 10 seconds of no messages
)

print(f"Reading from {topic_name}...")
print("Counting trips with trip_distance > 5.0 km...")

total_count = 0
distance_over_5_count = 0

for message in consumer:
    trip = message.value
    total_count += 1
    
    if trip.get('trip_distance') and trip['trip_distance'] > 5.0:
        distance_over_5_count += 1
    
    if total_count % 10000 == 0:
        print(f"Processed {total_count} trips, {distance_over_5_count} with distance > 5 km")

consumer.close()

print(f"\n=== RESULTS ===")
print(f"Total trips processed: {total_count}")
print(f"Trips with trip_distance > 5.0 km: {distance_over_5_count}")
