/* @bruin

name: reports.trips_report

type: duckdb.sql

depends:
  - staging.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: report_date
  time_granularity: date

columns:
  - name: report_date
    type: date
    description: Date of the trip activity
    primary_key: true
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: Type of taxi (yellow or green)
    primary_key: true
    checks:
      - name: not_null
  - name: total_trips
    type: bigint
    description: Total number of trips
    checks:
      - name: non_negative
  - name: total_revenue
    type: numeric
    description: Total revenue from all trips
    checks:
      - name: non_negative
  - name: avg_trip_distance
    type: numeric
    description: Average trip distance in miles
  - name: avg_fare_amount
    type: numeric
    description: Average fare amount per trip
  - name: total_passengers
    type: bigint
    description: Total number of passengers
    checks:
      - name: non_negative

custom_checks:
  - name: trips_count_positive
    description: Ensure each day has at least one trip
    query: |
      SELECT COUNT(*) > 0 FROM reports.trips_report WHERE total_trips > 0
    value: 1

@bruin */

-- Daily trips summary report aggregated by date and taxi type
-- Provides key metrics for dashboard and analytics
SELECT
    CAST(pickup_datetime AS DATE) as report_date,
    taxi_type,
    COUNT(*) as total_trips,
    SUM(total_amount) as total_revenue,
    ROUND(AVG(trip_distance), 2) as avg_trip_distance,
    ROUND(AVG(fare_amount), 2) as avg_fare_amount,
    SUM(passenger_count) as total_passengers,
    ROUND(AVG(tip_amount), 2) as avg_tip_amount,
    COUNT(DISTINCT VendorID) as unique_vendors
FROM staging.trips
WHERE CAST(pickup_datetime AS DATE) >= CAST('{{ start_datetime }}' AS DATE)
  AND CAST(pickup_datetime AS DATE) < CAST('{{ end_datetime }}' AS DATE)
GROUP BY 
    CAST(pickup_datetime AS DATE),
    taxi_type
ORDER BY report_date, taxi_type
