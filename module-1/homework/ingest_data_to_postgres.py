import pandas as pd
from tqdm.auto import tqdm
from sqlalchemy import create_engine

# # Convert parquet file to CSV
# parquet_filepath = "green_tripdata_2025-11.parquet"
# df_green_parquet = pd.read_parquet(parquet_filepath)
# df_green_parquet.to_csv("green_trips_2025-11.csv", index=False)

# Handle data types
dtype_green = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "trip_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "ehail_fee": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64",
    "cbd_congestion_fee": "float64"
}

dtype_zone = {
    "LocationID":"Int64",
    "Borough":"string",
    "Zone":"string",
    "service_zone":"string"
}


parse_dates = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime"
]

def ingest_green(filepath, engine, chunksize=100000):

    df_green_iter = pd.read_csv(
        filepath, 
        dtype=dtype_green, # pyright: ignore[reportArgumentType]
        parse_dates=parse_dates, 
        iterator=True, 
        chunksize=chunksize
    ) # type: ignore

    first_chunk = next(df_green_iter)

    first_chunk.head(0).to_sql(
        name="green_taxi_data",
        con=engine,
        if_exists="replace"
    )

    print("Table created")

    first_chunk.to_sql(
        name="green_taxi_data",
        con=engine,
        if_exists="append"
    )

    print("Inserted first chunk:", len(first_chunk))

    for df_chunk in df_green_iter:
        df_chunk.to_sql(
            name="green_taxi_data",
            con=engine,
            if_exists="append"
        )
        print("Inserted chunk:", len(df_chunk))


def ingest_zones(filepath, engine, chunksize=100000):

    df_zone_iter = pd.read_csv(
        filepath, 
        dtype=dtype_zone, # pyright: ignore[reportArgumentType]
        iterator=True, 
        chunksize=chunksize
    ) # type: ignore

    first_chunk = next(df_zone_iter)

    first_chunk.head(0).to_sql(
        name="taxi_zones",
        con=engine,
        if_exists="replace"
    )

    print("Table created")

    first_chunk.to_sql(
        name="taxi_zones",
        con=engine,
        if_exists="append"
    )

    print("Inserted first chunk:", len(first_chunk))

    for df_chunk in df_zone_iter:
        df_chunk.to_sql(
            name="taxi_zones",
            con=engine,
            if_exists="append"
        )
        print("Inserted chunk:", len(df_chunk))


def run():
    green_csv_url = "green_trips_2025-11.csv"
    zones_csv_url = "taxi_zone_lookup.csv"    

    engine = create_engine(f'postgresql://root:root@localhost:5432/ny_green')

    ingest_green(green_csv_url, engine)
    ingest_zones(zones_csv_url, engine)

if __name__ == '__main__':
    run()