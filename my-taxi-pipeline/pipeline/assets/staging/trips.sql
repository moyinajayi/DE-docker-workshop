/* @bruin

name: staging.trips

type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: taxi_type
    type: string
    description: Type of taxi (yellow or green)
    checks:
      - name: not_null
  - name: pickup_datetime
    type: timestamp
    description: Pickup datetime (normalized across yellow/green)
    checks:
      - name: not_null
  - name: extracted_at
    type: timestamp
    description: Timestamp when data was extracted
    checks:
      - name: not_null
  - name: trip_distance
    type: numeric
    description: Trip distance in miles
    checks:
      - name: non_negative
  - name: total_amount
    type: numeric
    description: Total amount charged to passengers

custom_checks:
  - name: row_count_positive
    description: Ensure the row count is positive
    query: |
      SELECT COUNT(*) > 0 FROM staging.trips
    value: 1

@bruin */

SELECT *
FROM ingestion.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
