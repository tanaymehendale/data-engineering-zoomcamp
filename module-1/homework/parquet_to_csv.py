import pandas as pd

# Convert parquet file to CSV
parquet_filepath = "green_tripdata_2025-11.parquet"
df_green_parquet = pd.read_parquet(parquet_filepath)
df_green_parquet.to_csv("green_trips_2025-11.csv", index=False)