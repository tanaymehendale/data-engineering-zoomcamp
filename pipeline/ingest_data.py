#!/usr/bin/env python
# coding: utf-8

import pandas as pd
# tqdm -- library to check progress on ingesting chunks
from tqdm.auto import tqdm
from sqlalchemy import create_engine


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


def run():        
    pg_user = 'root'
    pg_pass = 'root'
    pg_host = 'localhost'
    pg_port = '5432'
    pg_db = 'ny_taxi'
    chunksize=100000

    year = '2021'
    month = 1
    target_table = 'yellow_taxi_data'
    
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'

    engine = create_engine('postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')


    # Iterator --- Not a dataframe
    # Since the file is very big, if we try to insert all records into Postgres in one go, it will take a lot of time and resources,
    # For this, we ingest the file in chunks of equal size and we insert them one by one.
    df_iter = pd.read_csv(
        url, 
        dtype=dtype,  # type: ignore
        parse_dates=parse_dates, 
        iterator=True, 
        chunksize=chunksize
    ) # type: ignore

    first = True

    for df_chunk in tqdm(df_iter):
        if first:            
            df_chunk.head(0).to_sql(
                name=target_table, 
                con=engine, 
                if_exists='replace'
            )
            first = False
        
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists='append'
        )

if __name__ == '__main__':
    run()