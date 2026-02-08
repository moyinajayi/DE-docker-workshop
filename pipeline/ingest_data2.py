#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

# Data types for green taxi data
green_taxi_dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

green_taxi_parse_dates = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime"
]

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--data-type', type=click.Choice(['green_taxi', 'taxi_zones']), required=True, help='Type of data to ingest')
@click.option('--year', default=2025, type=int, help='Year for taxi data (green taxi only)')
@click.option('--month', default=11, type=int, help='Month for taxi data (green taxi only)')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading')
def run(pg_user, pg_pass, pg_host, pg_db, pg_port, data_type, year, month, chunksize):
    """Ingest NYC taxi data into PostgreSQL database."""
    
    engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    if data_type == 'green_taxi':
        ingest_green_taxi(engine, year, month, chunksize)
    elif data_type == 'taxi_zones':
        ingest_taxi_zones(engine)

def ingest_green_taxi(engine, year, month, chunksize):
    """Ingest green taxi parquet data."""
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02d}.parquet'
    target_table = f'green_taxi_trips_{year}_{month:02d}'
    
    click.echo(f'Ingesting green taxi data from {url}')
    
    df_iter = pd.read_parquet(
        url,
        engine='pyarrow'
    )
    
    # Convert to chunks manually since read_parquet doesn't have iterator
    num_chunks = len(df_iter) // chunksize + (1 if len(df_iter) % chunksize else 0)
    
    first = True
    for i in tqdm(range(0, len(df_iter), chunksize), total=num_chunks, desc='Ingesting chunks'):
        df_chunk = df_iter.iloc[i:i+chunksize]
        
        if first:
            df_chunk.head(0).to_sql(name=target_table, con=engine, if_exists='replace', index=False)
            first = False
        
        df_chunk.to_sql(name=target_table, con=engine, if_exists='append', index=False)
    
    click.echo(f'✓ Ingested {len(df_iter)} rows into {target_table}')

def ingest_taxi_zones(engine):
    """Ingest taxi zone lookup data."""
    url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'
    target_table = 'taxi_zones'
    
    click.echo(f'Ingesting taxi zones from {url}')
    
    df = pd.read_csv(url)
    df.to_sql(name=target_table, con=engine, if_exists='replace', index=False)
    
    click.echo(f'✓ Ingested {len(df)} rows into {target_table}')

if __name__ == '__main__':
    run()