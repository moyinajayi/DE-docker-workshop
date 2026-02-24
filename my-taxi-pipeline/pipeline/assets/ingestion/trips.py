"""@bruin


name: ingestion.trips
type: python
image: python:3.11

connection: duckdb-default

materialization:
  type: table
  strategy: create+replace

@bruin"""

import os
import json
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta


def materialize():
    """
    Fetch NYC Taxi trip data from TLC public endpoint.
    
    Uses BRUIN_START_DATE, BRUIN_END_DATE to determine date range.
    Uses taxi_types variable from BRUIN_VARS to determine which taxi types to ingest.
    
    Returns a DataFrame with raw trip data plus an extracted_at timestamp.
    """
    # Read Bruin environment variables
    start_date = os.environ.get("BRUIN_START_DATE")
    end_date = os.environ.get("BRUIN_END_DATE")
    vars_json = os.environ.get("BRUIN_VARS", "{}")
    
    # Parse variables to get taxi_types
    variables = json.loads(vars_json)
    taxi_types = variables.get("taxi_types", ["yellow", "green"])
    
    # Parse dates
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    # Base URL for NYC TLC data
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    
    # Generate list of URLs to fetch
    dataframes = []
    current = start
    
    while current < end:
        year_month = current.strftime("%Y-%m")
        
        for taxi_type in taxi_types:
            # Construct URL: <taxi_type>_tripdata_<year>-<month>.parquet
            url = f"{base_url}{taxi_type}_tripdata_{year_month}.parquet"
            
            try:
                # Define which columns to read based on taxi type
                if taxi_type == "yellow":
                    columns_to_read = [
                        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
                        "passenger_count", "trip_distance", "RatecodeID",
                        "store_and_fwd_flag", "PULocationID", "DOLocationID",
                        "payment_type", "fare_amount", "extra", "mta_tax",
                        "tip_amount", "tolls_amount", "improvement_surcharge",
                        "total_amount"
                    ]
                else:  # green
                    columns_to_read = [
                        "VendorID", "lpep_pickup_datetime", "lpep_dropoff_datetime",
                        "passenger_count", "trip_distance", "RatecodeID",
                        "store_and_fwd_flag", "PULocationID", "DOLocationID",
                        "payment_type", "fare_amount", "extra", "mta_tax",
                        "tip_amount", "tolls_amount", "improvement_surcharge",
                        "total_amount"
                    ]
                
                # Fetch only the specified columns
                df = pd.read_parquet(url, columns=columns_to_read)
                
                # Rename to standard column names
                if taxi_type == "yellow":
                    df = df.rename(columns={
                        "VendorID": "vendor_id",
                        "tpep_pickup_datetime": "pickup_datetime",
                        "tpep_dropoff_datetime": "dropoff_datetime",
                        "RatecodeID": "ratecode_id",
                        "PULocationID": "pulocation_id",
                        "DOLocationID": "dolocation_id",
                    })
                else:  # green
                    df = df.rename(columns={
                        "VendorID": "vendor_id",
                        "lpep_pickup_datetime": "pickup_datetime",
                        "lpep_dropoff_datetime": "dropoff_datetime",
                        "RatecodeID": "ratecode_id",
                        "PULocationID": "pulocation_id",
                        "DOLocationID": "dolocation_id",
                    })
                
                # Add metadata columns
                df["taxi_type"] = taxi_type
                df["extracted_at"] = datetime.now()
                
                dataframes.append(df)
                print(f"Successfully fetched: {url} ({len(df)} rows)")
                
            except Exception as e:
                print(f"Failed to fetch {url}: {e}")
        
        # Move to next month
        current += relativedelta(months=1)
    
    # Concatenate all dataframes
    if dataframes:
        final_df = pd.concat(dataframes, ignore_index=True)
        
        # Ensure only desired columns are present (remove any extra columns) 
        desired_columns = [
            "vendor_id", "pickup_datetime", "dropoff_datetime",
            "passenger_count", "trip_distance", "ratecode_id",
            "store_and_fwd_flag", "pulocation_id", "dolocation_id",
            "payment_type", "fare_amount", "extra", "mta_tax",
            "tip_amount", "tolls_amount", "improvement_surcharge",
            "total_amount", "taxi_type", "extracted_at"
        ]
        
        # Select only the columns we want, in the order we want
        final_df = final_df[desired_columns]
        
        print(f"Total rows fetched: {len(final_df)}")
        print(f"Final columns: {list(final_df.columns)}")
        return final_df
    else:
        # Return empty DataFrame if nothing was fetched
        print("No data fetched")
        return pd.DataFrame()


