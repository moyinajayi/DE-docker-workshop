import json
import time
import pandas as pd
from kafka import KafkaProducer


def json_serializer(data):
    return json.dumps(data).encode('utf-8')


# Download Green Taxi Trip data for October 2025
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
columns = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount',
    'total_amount'
]

print("Downloading green taxi data...")
df = pd.read_parquet(url, columns=columns)
print(f"Loaded {len(df)} rows")

# Connect to Kafka
server = 'localhost:9092'
producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

topic_name = 'green-trips'

print(f"Sending data to {topic_name}...")
t0 = time.time()

for _, row in df.iterrows():
    # Convert to dict and handle datetime columns
    record = {
        'lpep_pickup_datetime': str(row['lpep_pickup_datetime']),
        'lpep_dropoff_datetime': str(row['lpep_dropoff_datetime']),
        'PULocationID': int(row['PULocationID']) if pd.notna(row['PULocationID']) else None,
        'DOLocationID': int(row['DOLocationID']) if pd.notna(row['DOLocationID']) else None,
        'passenger_count': float(row['passenger_count']) if pd.notna(row['passenger_count']) else None,
        'trip_distance': float(row['trip_distance']) if pd.notna(row['trip_distance']) else None,
        'tip_amount': float(row['tip_amount']) if pd.notna(row['tip_amount']) else None,
        'total_amount': float(row['total_amount']) if pd.notna(row['total_amount']) else None,
    }
    producer.send(topic_name, value=record)

producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')
