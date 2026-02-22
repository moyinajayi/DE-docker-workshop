"""@bruin


name: ingestion.trips
type: python
image: python:3.11

connection: duckdb-default

materialization:
  type: table
  strategy: append

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
                # Fetch and parse parquet file
                df = pd.read_parquet(url)
                
                # Add metadata columns for lineage tracking
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
        print(f"Total rows fetched: {len(final_df)}")
        return final_df
    else:
        # Return empty DataFrame if nothing was fetched
        print("No data fetched")
        return pd.DataFrame()


