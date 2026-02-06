#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm



dtype = {
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

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

df = pd.read_csv(
    url,
    dtype=dtype,
    parse_dates=parse_dates
)


# Read a sample of the data
prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz' 
engine = create_engine('postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')



df.head(0).to_sql(name='yellow_taxi_data', con=engine, if_exists = 'replace')
#df.to_sql(name='yellow_taxi_data', con=engine))

def run():

    pg_user = 'root'
    pg_pass = 'root'
    pg_host 'localhost'
    pg_db = 'ny_taxi'
    pg_port = 5432

    year = 2021
    month = 1

    table_name = 'yellow_taxi '

    chunksize = 100000

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )

    first = True
    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(0).to_sql(
                name='yellow_taxi_data', 
                con=engine, 
                if_exists='replace'
                )
            first = False
            
            df_chunk.to_sql(
                name='yellow_taxi_data', 
                con=engine, 
                if_exists='append')


# In[38]:


df_chunk.head()


# In[42]:


for df_chunk in df_iter:
    print(len(df_chunk))


# In[55]:


TABLE = "yellow_taxi_data"
READ_CHUNK_SIZE = 100_000
WRITE_CHUNK_SIZE = 10_000


# In[56]:


def make_df_iter():
    return pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        chunksize=READ_CHUNK_SIZE
    )


# In[57]:


df_iter = make_df_iter()
first_chunk = next(df_iter)
print(len(first_chunk))
display(first_chunk.head(3))


# In[58]:


df_iter = make_df_iter()


# In[60]:


from tqdm.auto import tqdm

TABLE = "yellow_taxi_data"
READ_CHUNK = 100_000
WRITE_CHUNK = 2_000  # <-- lower this

df_iter = make_df_iter()  # recreate iterator

with engine.begin() as conn:
    for i, df_chunk in enumerate(tqdm(df_iter, desc="Writing"), start=1):
        df_chunk.to_sql(
            name=TABLE,
            con=conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=WRITE_CHUNK
        )


# In[ ]:




