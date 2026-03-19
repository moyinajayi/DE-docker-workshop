-- Create tables for Flink job results

-- Question 4: Tumbling window pickup counts
CREATE TABLE IF NOT EXISTS pickup_counts (
    window_start TIMESTAMP,
    PULocationID INT,
    num_trips BIGINT,
    PRIMARY KEY (window_start, PULocationID)
);

-- Question 5: Session window streak counts
CREATE TABLE IF NOT EXISTS session_counts (
    session_start TIMESTAMP,
    session_end TIMESTAMP,
    PULocationID INT,
    num_trips BIGINT,
    PRIMARY KEY (session_start, PULocationID)
);

-- Question 6: Hourly tip amounts
CREATE TABLE IF NOT EXISTS hourly_tips (
    window_start TIMESTAMP PRIMARY KEY,
    total_tip_amount DOUBLE PRECISION
);
