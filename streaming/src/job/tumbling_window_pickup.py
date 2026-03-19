"""
Question 4: Tumbling window - pickup location
5-minute tumbling window to count trips per PULocationID
"""
import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings


def main():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # green-trips topic has 1 partition
    
    # Add connector JARs
    lib_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'lib')
    jar_files = [
        f"file://{os.path.join(lib_dir, 'flink-sql-connector-kafka-3.3.0-1.20.jar')}",
        f"file://{os.path.join(lib_dir, 'flink-connector-jdbc-3.2.0-1.19.jar')}",
        f"file://{os.path.join(lib_dir, 'postgresql-42.7.3.jar')}",
    ]
    env.add_jars(*jar_files)
    
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    
    # Create Kafka source table
    t_env.execute_sql("""
        CREATE TABLE green_trips (
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            PULocationID INT,
            DOLocationID INT,
            passenger_count DOUBLE,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            total_amount DOUBLE,
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'green-trips',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'flink-tumbling-window',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """)
    
    # Create PostgreSQL sink table
    t_env.execute_sql("""
        CREATE TABLE pickup_counts (
            window_start TIMESTAMP(3),
            PULocationID INT,
            num_trips BIGINT,
            PRIMARY KEY (window_start, PULocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://localhost:5432/postgres',
            'table-name' = 'pickup_counts',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """)
    
    # 5-minute tumbling window aggregation
    result = t_env.execute_sql("""
        INSERT INTO pickup_counts
        SELECT 
            TUMBLE_START(event_timestamp, INTERVAL '5' MINUTE) AS window_start,
            PULocationID,
            COUNT(*) AS num_trips
        FROM green_trips
        WHERE PULocationID IS NOT NULL
        GROUP BY 
            TUMBLE(event_timestamp, INTERVAL '5' MINUTE),
            PULocationID
    """)
    
    # Wait for the streaming job to execute
    result.wait()


if __name__ == '__main__':
    main()
